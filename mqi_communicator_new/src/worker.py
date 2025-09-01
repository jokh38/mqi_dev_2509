"""
Entry point for a single worker process.
Each worker handles exactly one case from start to finish.
"""
import sys
import os
from pathlib import Path
from multiprocessing import Queue
from typing import NoReturn

# Add project root to path to allow absolute imports
project_root = Path(__file__).resolve().parent.parent
sys.path.append(str(project_root))

from src.config import ConfigManager
from src.database_handler import DatabaseHandler
from src.logging_handler import LoggingHandler, LogContext
from src.local_handler import LocalHandler
from src.remote_handler import RemoteHandler
from src.workflow_manager import WorkflowManager


def worker_main(case_id: str, case_path_str: str, status_queue: Queue = None) -> NoReturn:
    """
    Entry point for a worker process that handles a single case.
    """
    # Configuration and Logging
    config_manager = ConfigManager("config/config.yaml")
    config = config_manager.get_config()
    logging_handler = LoggingHandler()
    logger = logging_handler.get_worker_logger(os.getpid())
    log_context = LogContext(case_id=case_id)

    try:
        logger.info(f"Worker (PID: {os.getpid()}) starting for case {case_id}", log_context)

        # Initialize handlers
        db_handler = DatabaseHandler(config.paths.local.scan_directory + "/../database/mqi_communicator.db")
        local_handler = LocalHandler(config)
        remote_handler = RemoteHandler(config)

        # Get case path
        case_path = Path(case_path_str)

        # Create and run workflow
        workflow = WorkflowManager(
            case_id=case_id,
            case_path=case_path,
            config=config,
            db_handler=db_handler,
            local_handler=local_handler,
            remote_handler=remote_handler,
            logger=logger,
            status_queue=status_queue
        )
        workflow.run_workflow()

        final_status = workflow.get_current_status()
        logger.info(f"Worker finished for case {case_id} with status: {final_status}", log_context)
        sys.exit(0)

    except Exception as e:
        logger.error(f"Worker for case {case_id} failed with unhandled exception: {e}", log_context)
        # The workflow itself should have sent a "Failed" status update.
        # We ensure the DB is updated as a fallback.
        db = DatabaseHandler(config.paths.local.scan_directory + "/../database/mqi_communicator.db")
        db.update_case_status(case_id, "FAILED")
        db.close()
        sys.exit(1)


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python worker.py <case_id> <case_path>")
        sys.exit(1)

    case_id_arg = sys.argv[1]
    case_path_arg = sys.argv[2]

    # In a real scenario, the queue would be passed by the master process
    # For standalone testing, we can create a dummy queue.
    dummy_queue = Queue()

    worker_main(case_id_arg, case_path_arg, dummy_queue)