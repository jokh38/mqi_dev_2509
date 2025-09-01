import sys
import time
import os
import subprocess
from datetime import datetime, timezone, timedelta
from typing import Optional, Dict, Any
from logging.handlers import RotatingFileHandler
from logging import getLogger, INFO, StreamHandler, LogRecord

from src.common.db_manager import DatabaseManager
from src.common.config_manager import ConfigManager, ConfigValidationError
from src.common.structured_logging import get_structured_logger, LogContext, JsonFormatter
from src.services.case_scanner import CaseScanner
from src.services.workflow_engine import WorkflowEngine
from src.services.dynamic_gpu_manager import DynamicGpuManager
from src.services.priority_scheduler import PriorityScheduler, PriorityConfig
from src.services.parallel_processor import ParallelCaseProcessor
from src.services.main_loop_logic import (
    recover_stuck_submitting_cases,
    manage_running_cases,
    manage_zombie_resources,
    process_new_submitted_cases_parallel,
    process_new_submitted_cases_with_optimization,
)

# Define the path to the configuration file
CONFIG_PATH = "config/config.yaml"

# Define Korea Standard Time (KST)
KST = timezone(timedelta(hours=9))




def setup_logging(config: Dict[str, Any]) -> None:
    """Sets up structured, file-based, timezone-aware logging for the application."""
    log_config = config.get("logging", {})
    log_path = log_config.get("path", "communicator_fallback.log")

    log_formatter = JsonFormatter(KST)
    log_handler = RotatingFileHandler(log_path, maxBytes=5 * 1024 * 1024, backupCount=5)
    log_handler.setFormatter(log_formatter)

    root_logger = getLogger()
    root_logger.setLevel(INFO)  # Restored to INFO for production
    root_logger.addHandler(log_handler)

    # Add a console handler for immediate feedback
    console_handler = StreamHandler()
    console_handler.setFormatter(log_formatter)
    root_logger.addHandler(console_handler)

    # Log configuration completion using structured logging
    setup_logger = get_structured_logger("setup", {"component": "logging_setup"})
    setup_logger.info(f"Structured logger has been configured. Logging to: {log_path}")


def main(config: Dict[str, Any]) -> None:
    """
    Enhanced main function with parallel processing and dynamic GPU management.
    This function initializes all components and runs the optimized main loop.
    """
    case_scanner = None
    db_manager = None
    dashboard_process = None
    gpu_manager = None
    parallel_processor = None
    priority_scheduler = None

    # Initialize structured logger for main function
    logger = get_structured_logger("main", {"component": "main_application"})
    
    try:
        logger.info("MQI Communicator application starting...")

        # 1. Initialize Components & DB
        db_manager = DatabaseManager(config=config)
        db_manager.init_db()
        logger.info("DatabaseManager initialized.")

        # 2. Start dashboard if configured to do so
        dashboard_config = config.get("dashboard", {})
        if dashboard_config.get("auto_start", False):
            try:
                # Launch dashboard as a separate process
                # On Windows, create a new console window for the dashboard.
                # On other systems, it will inherit the console.
                creationflags = 0
                if sys.platform == "win32":
                    creationflags = subprocess.CREATE_NEW_CONSOLE

                dashboard_process = subprocess.Popen(
                    [sys.executable, "-m", "src.dashboard"],
                    creationflags=creationflags,
                )
                logger.info("Dashboard started as separate process.")
            except Exception as e:
                logger.warning_with_exception("Failed to start dashboard", e)

        # 3. Initialize GPU Resources from Config
        pueue_config = config.get("pueue", {})
        pueue_groups = pueue_config.get("groups", [])
        if not pueue_groups:
            raise ValueError("Config error: 'pueue.groups' must be a non-empty list.")

        for group in pueue_groups:
            db_manager.ensure_gpu_resource_exists(group)
            logger.info(f"Ensured GPU resource for group '{group}' exists.", 
                       LogContext(gpu_group=group, operation="resource_initialization"))

        # 4. Initialize Dynamic GPU Manager
        try:
            gpu_manager = DynamicGpuManager(config=config, db_manager=db_manager)
            logger.info(
                "DynamicGpuManager initialized for optimal resource allocation."
            )

            # Initial GPU resource discovery
            gpu_manager.refresh_gpu_resources()
        except Exception as e:
            logger.warning_with_exception(
                "Failed to initialize DynamicGpuManager. Using static configuration", e
            )

        # 5. Initialize Priority Scheduler (if enabled)
        main_loop_config = config.get("main_loop", {})
        priority_config_dict = main_loop_config.get("priority_scheduling", {})
        if priority_config_dict.get("enabled", False):
            try:
                # Create PriorityConfig from the dictionary in config.yaml
                p_config = PriorityConfig(
                    algorithm=priority_config_dict.get("algorithm", "weighted_fair"),
                    aging_factor=priority_config_dict.get("aging_factor", 0.1),
                    starvation_threshold_hours=priority_config_dict.get(
                        "starvation_threshold_hours", 24
                    ),
                )
                priority_scheduler = PriorityScheduler(
                    db_manager=db_manager, config=p_config
                )
                logger.info(
                    f"Priority scheduling enabled with algorithm: {p_config.algorithm}"
                )
            except Exception as e:
                logger.warning_with_exception(
                    "Failed to initialize priority scheduler. Priority scheduling disabled", e
                )
                priority_scheduler = None

        # 6. Initialize Parallel Processing (if enabled)
        parallel_config = main_loop_config.get("parallel_processing", {})
        if parallel_config.get("enabled", False):
            try:
                parallel_processor = ParallelCaseProcessor(
                    db_manager=db_manager,
                    workflow_engine=None,  # Will be set after WorkflowEngine creation
                    gpu_manager=gpu_manager,
                    priority_scheduler=priority_scheduler,
                    max_workers=parallel_config.get("max_workers", 4),
                    batch_size=parallel_config.get("batch_size", 10),
                    processing_timeout=parallel_config.get("processing_timeout", 300.0),
                )
                logger.info(
                    f"Parallel processing enabled with {parallel_processor.max_workers} workers, "
                    f"priority scheduling: {'enabled' if priority_scheduler else 'disabled'}."
                )
            except Exception as e:
                logger.warning_with_exception(
                    "Failed to initialize parallel processor. Using sequential processing", e
                )
                parallel_processor = None

        # 7. Continue Component Initialization
        watch_path = config.get("scanner", {}).get("watch_path", "new_cases")
        sleep_interval = main_loop_config.get("sleep_interval_seconds", 10)
        running_case_timeout_hours = main_loop_config.get(
            "running_case_timeout_hours", 24
        )
        timeout_delta = timedelta(hours=running_case_timeout_hours)

        # Ensure the watch path exists before starting the scanner
        os.makedirs(watch_path, exist_ok=True)
        logger.info(f"Ensured watch directory exists: {watch_path}")

        workflow_engine = WorkflowEngine(config=config)
        logger.info("WorkflowEngine initialized.")

        # Set workflow_engine for parallel processor if it exists
        if parallel_processor:
            parallel_processor.workflow_engine = workflow_engine

        case_scanner = CaseScanner(
            watch_path=watch_path, db_manager=db_manager, config=config
        )
        logger.info("CaseScanner initialized.")

        # 8. Perform initial scan and start background services
        # The scanner will first check for any pre-existing cases before starting to watch for new ones.
        case_scanner.perform_initial_scan()
        case_scanner.start()

        # 9. Main Application Loop
        logger.info(
            "Starting enhanced main application loop with parallel processing and dynamic GPU management..."
        )
        loop_iteration = 0
        gpu_refresh_interval = main_loop_config.get(
            "gpu_refresh_interval_iterations", 50
        )
        db_optimization_interval = main_loop_config.get(
            "db_optimization_interval_iterations", 1000
        )

        while True:
            try:
                loop_iteration += 1

                # Periodically refresh GPU resources for optimal allocation
                if gpu_manager and loop_iteration % gpu_refresh_interval == 0:
                    try:
                        gpu_manager.refresh_gpu_resources()
                        logger.info("GPU resources refreshed for optimal allocation")
                    except Exception as e:
                        logger.warning_with_exception("GPU resource refresh failed", e)

                # Periodically optimize the database
                if db_manager and loop_iteration % db_optimization_interval == 0:
                    try:
                        logger.info("Starting periodic database optimization...")
                        start_time = time.time()
                        db_manager.optimize_database()
                        duration = time.time() - start_time
                        logger.info(f"Database optimization completed in {duration:.2f} seconds.")
                    except Exception as e:
                        logger.warning_with_exception("Database optimization failed", e)

                # The core logic with enhanced parallel processing
                recover_stuck_submitting_cases(db_manager, workflow_engine)
                manage_running_cases(db_manager, workflow_engine, timeout_delta, KST)
                manage_zombie_resources(db_manager, workflow_engine)

                cases_processed = 0
                # Use parallel processing if available, otherwise fall back to sequential
                if parallel_processor:
                    try:
                        cases_processed = process_new_submitted_cases_parallel(
                            db_manager, workflow_engine, parallel_processor
                        )
                    except Exception as e:
                        logger.error_with_exception(
                            "Parallel processing error. Falling back to sequential", e
                        )
                        process_new_submitted_cases_with_optimization(
                            db_manager, workflow_engine, gpu_manager
                        )
                else:
                    # Use optimized sequential processing with dynamic GPU management
                    process_new_submitted_cases_with_optimization(
                        db_manager, workflow_engine, gpu_manager
                    )

                # Log performance metrics periodically
                if loop_iteration % 10 == 0:
                    if parallel_processor and cases_processed > 0:
                        metrics = parallel_processor.get_performance_summary()
                        logger.info(
                            f"Parallel processing metrics: {metrics['total_cases_processed']} cases, "
                            f"{metrics['success_rate_percent']}% success rate, "
                            f"{metrics['average_processing_time_seconds']}s avg time"
                        )
                    # Log DB performance metrics, now available from the new DatabaseManager
                    db_metrics = db_manager.get_performance_metrics()
                    logger.info(f"DB Performance: {db_metrics}")

            except Exception as e:
                # Catch exceptions in the main loop itself to prevent crashing
                logger.error_with_exception(
                    "An unexpected error occurred in the main loop", e
                )

            time.sleep(sleep_interval)

    except KeyboardInterrupt:
        logger.info("Shutdown signal received (KeyboardInterrupt).")
    except Exception as e:
        logger.error_with_exception(
            "A critical unhandled exception occurred in main", e
        )
    finally:
        logger.info("Initiating graceful shutdown...")

        # Log final performance metrics if parallel processing was used
        if parallel_processor:
            try:
                final_metrics = parallel_processor.get_performance_summary()
                logger.info(f"Final parallel processing metrics: {final_metrics}")
            except Exception as e:
                logger.warning_with_exception("Failed to log final metrics", e)

        if priority_scheduler:
            try:
                final_metrics = priority_scheduler.get_priority_statistics()
                logger.info(f"Final priority scheduling metrics: {final_metrics}")
            except Exception as e:
                logger.warning_with_exception("Failed to log final priority metrics", e)

        if case_scanner and case_scanner.observer.is_alive():
            case_scanner.stop()
            logger.info("CaseScanner stopped.")
        if db_manager:
            db_manager.close()
            logger.info("Database connection closed.")
        if dashboard_process:
            try:
                dashboard_process.terminate()
                dashboard_process.wait(timeout=5)
                logger.info("Dashboard process terminated.")
            except subprocess.TimeoutExpired:
                dashboard_process.kill()
                logger.warning("Dashboard process killed after timeout.")
            except Exception as e:
                logger.error_with_exception("Error terminating dashboard process", e)
        logger.info("MQI Communicator application has shut down.")


if __name__ == "__main__":
    # Load config using ConfigManager for logging setup before the main function
    try:
        config_manager = ConfigManager(CONFIG_PATH)
        initial_config = config_manager.config
        setup_logging(initial_config)
    except ConfigValidationError as e:
        print(f"ERROR: Configuration validation failed: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"ERROR: Failed to load configuration: {e}")
        sys.exit(1)

    main(initial_config)
