import subprocess
import shlex
import re
import json
import logging
import uuid
import time
from pathlib import Path
from typing import Dict, Any, Optional, Literal, List

from src.common.structured_logging import get_structured_logger, LogContext
from src.common.error_categorization import BaseExecutionError
from src.common.dicom_parser import find_rtplan_file, get_plan_info
from src.common.db_manager import DatabaseManager
from src.common.error_categorization import categorize_error
from src.common.rich_display import create_progress_display
from src.services.local_executor import LocalExecutor, LocalExecutionError
from src.services.remote_executor import RemoteExecutor, RemoteExecutionError
from src.services.tps_generator import create_ini_content

logger = get_structured_logger(__name__)


class WorkflowExecutionError(BaseExecutionError):
    """Custom exception for errors during workflow execution."""
    
    def __init__(self, message: str, step_name: Optional[str] = None, error_category: Optional[str] = None):
        details = {}
        if step_name:
            details["step_name"] = step_name
        if error_category:
            details["error_category"] = error_category
        super().__init__(message, details)


class WorkflowEngine:
    """
    State-driven workflow engine for managing the complete lifecycle of case processing.
    
    This class orchestrates the execution of configurable workflows by delegating
    to specialized executors (LocalExecutor, RemoteExecutor) and managing state
    transitions, error recovery, and progress display.
    """

    def __init__(self, config: Dict[str, Any], db_manager: Optional[DatabaseManager] = None) -> None:
        """
        Initialize the WorkflowEngine with configuration and database manager.

        Args:
            config: Complete application configuration dictionary
            db_manager: Optional database manager for state persistence (backward compatibility)
        """
        self.config = config
        self.db_manager = db_manager
        
        # Initialize executors
        self.local_executor = LocalExecutor(config)
        self.remote_executor = RemoteExecutor(config)
        
        # Workflow configuration
        self.main_workflow = config.get("main_workflow", [])
        
        # Legacy HPC config for backward compatibility
        self.hpc_config: Dict[str, Any] = config.get("hpc", {})
        self.user = self.hpc_config.get("user")
        self.host = self.hpc_config.get("host")
        self.ssh_cmd = self.hpc_config.get("ssh_command", "ssh")
        self.pueue_cmd = self.hpc_config.get("pueue_command", "pueue")

    def process_case(self, case_id: int, case_path: str, pueue_group: str = "default") -> bool:
        """
        Process a case through the complete workflow based on its current state.
        
        This is the main entry point for the workflow engine. It:
        1. Queries the database for the case's current status
        2. Determines the appropriate starting step
        3. Executes the workflow steps with error handling and recovery
        4. Provides real-time progress display
        
        Args:
            case_id: Database ID of the case
            case_path: Local path to the case directory
            pueue_group: Pueue group for remote execution
            
        Returns:
            True if the workflow completed successfully, False otherwise
        """
        case_name = Path(case_path).name
        
        logger.info(
            f"Starting workflow processing for case {case_name}",
            context=LogContext(
                case_id=str(case_id),
                operation="workflow_processing",
                extra_data={
                    "case_path": case_path,
                    "pueue_group": pueue_group
                }
            )
        )
        
        # Create progress display
        with create_progress_display(case_name, case_id) as display:
            try:
                # Step 1: Determine starting point based on current case status
                current_status = "NEW"  # Default status
                if self.db_manager:
                    current_status = self.db_manager.get_case_status(case_id)
                starting_step_index = self._determine_starting_step(current_status)
                
                logger.info(
                    f"Case current status: {current_status}, starting from step {starting_step_index}",
                    context=LogContext(
                        case_id=str(case_id),
                        operation="workflow_planning",
                        extra_data={
                            "current_status": current_status,
                            "starting_step": starting_step_index
                        }
                    ).to_dict()
                )
                
                # Step 2: Execute workflow steps
                for step_index, step_config in enumerate(self.main_workflow[starting_step_index:], starting_step_index):
                    success = self._execute_workflow_step(
                        step_config, 
                        case_id, 
                        case_path, 
                        pueue_group,
                        display
                    )
                    
                    if not success:
                        logger.error(
                            f"Workflow failed at step: {step_config['name']}",
                            context=LogContext(
                                case_id=str(case_id),
                                operation="workflow_processing",
                                extra_data={"failed_step": step_config["name"]}
                            ).to_dict()
                        )
                        return False
                
                logger.info(
                    f"Workflow completed successfully for case {case_name}",
                    context=LogContext(
                        case_id=str(case_id),
                        operation="workflow_processing",
                        extra_data={"final_status": "completed"}
                    ).to_dict()
                )
                
                return True
                
            except Exception as e:
                error_category = categorize_error(e, "workflow_processing")
                logger.error_with_exception(
                    f"Workflow processing failed for case {case_name}",
                    e,
                    context=LogContext(
                        case_id=str(case_id),
                        operation="workflow_processing",
                        error_category=error_category,
                        extra_data={"case_path": case_path}
                    ).to_dict()
                )
                return False

    def _determine_starting_step(self, current_status: str) -> int:
        """
        Determine which workflow step to start from based on current case status.
        
        Args:
            current_status: Current status from the database
            
        Returns:
            Index of the workflow step to start from
        """
        # Map statuses to step indices
        status_to_step = {}
        
        for index, step in enumerate(self.main_workflow):
            # If current status matches a completion status, start from next step
            if current_status == step.get('on_success_status'):
                return min(index + 1, len(self.main_workflow))
            
            # Store mapping for failure recovery
            status_to_step[step.get('on_failure_status')] = index
        
        # If status indicates failure, restart from that step
        if current_status in status_to_step:
            return status_to_step[current_status]
        
        # Default: start from the beginning
        return 0

    def _execute_workflow_step(
        self, 
        step_config: Dict[str, Any], 
        case_id: int,
        case_path: str, 
        pueue_group: str,
        display
    ) -> bool:
        """
        Execute a single workflow step with retry logic and error handling.
        
        Args:
            step_config: Configuration for the workflow step
            case_id: Database ID of the case
            case_path: Local path to the case directory
            pueue_group: Pueue group for remote execution
            display: Progress display instance
            
        Returns:
            True if step completed successfully, False otherwise
        """
        step_name = step_config['name']
        step_type = step_config['type']
        target = step_config['target']
        
        # Add step to display
        display.add_step(step_name)
        display.start_step(step_name)
        
        # Update database status to indicate step start
        on_start_status = step_config.get('on_start_status')
        if on_start_status and self.db_manager:
            self.db_manager.update_case_status(case_id, on_start_status)
        
        logger.info(
            f"Executing workflow step: {step_name}",
            context=LogContext(
                case_id=str(case_id),
                operation="step_execution",
                extra_data={
                    "step_name": step_name,
                    "step_type": step_type,
                    "target": target
                }
            ).to_dict()
        )
        
        # Prepare execution context
        run_id = str(uuid.uuid4())[:8]  # Short unique ID for this attempt
        context = {
            'case_id': case_id,
            'case_path': case_path,
            'pueue_group': pueue_group,
            'run_id': run_id,
            'step_config': step_config
        }
        
        # Execute step with retry logic
        retry_config = step_config.get('retry', {})
        max_retries = retry_config.get('count', 0)
        retry_delay = retry_config.get('delay', 60)
        retry_on_errors = retry_config.get('on_error', [])
        
        for attempt in range(max_retries + 1):
            try:
                # Execute the step based on its type
                if step_type == 'local':
                    result = self.local_executor.execute(target, context, display)
                elif step_type == 'remote':
                    result = self.remote_executor.execute(target, context, display)
                else:
                    raise WorkflowExecutionError(
                        f"Unknown step type: {step_type}",
                        step_name=step_name,
                        error_category="configuration"
                    )
                
                # Step completed successfully
                display.complete_step()
                
                # Update database status
                on_success_status = step_config.get('on_success_status')
                if on_success_status and self.db_manager:
                    self.db_manager.update_case_status(case_id, on_success_status)
                
                logger.info(
                    f"Step {step_name} completed successfully",
                    context=LogContext(
                        case_id=str(case_id),
                        operation="step_execution",
                        extra_data={
                            "step_name": step_name,
                            "attempt": attempt + 1,
                            "result": result
                        }
                    ).to_dict()
                )
                
                return True
                
            except (LocalExecutionError, RemoteExecutionError) as e:
                error_category = categorize_error(e, f"{step_type}_execution")
                
                # Check if this error type is retryable
                should_retry = (
                    attempt < max_retries and
                    (not retry_on_errors or error_category in retry_on_errors)
                )
                
                if should_retry:
                    logger.warning(
                        f"Step {step_name} failed (attempt {attempt + 1}/{max_retries + 1}), retrying in {retry_delay}s",
                        context=LogContext(
                            case_id=str(case_id),
                            operation="step_retry",
                            extra_data={
                                "step_name": step_name,
                                "attempt": attempt + 1,
                                "error_category": error_category,
                                "retry_delay": retry_delay
                            }
                        ).to_dict()
                    )
                    
                    display.update_status(f"Retrying in {retry_delay}s (attempt {attempt + 2}/{max_retries + 1})")
                    time.sleep(retry_delay)
                    
                    # Generate new run_id for retry to ensure idempotency
                    context['run_id'] = str(uuid.uuid4())[:8]
                    continue
                else:
                    # No more retries, mark as failed
                    error_msg = f"Step {step_name} failed after {attempt + 1} attempts: {str(e)}"
                    display.set_error(error_msg)
                    
                    # Update database status
                    on_failure_status = step_config.get('on_failure_status')
                    if on_failure_status and self.db_manager:
                        self.db_manager.update_case_status(case_id, on_failure_status, str(e))
                    
                    logger.error_with_exception(
                        f"Step {step_name} failed permanently",
                        e,
                        context=LogContext(
                            case_id=str(case_id),
                            operation="step_execution",
                            error_category=error_category,
                            extra_data={
                                "step_name": step_name,
                                "total_attempts": attempt + 1
                            }
                        ).to_dict()
                    )
                    
                    return False
            
            except Exception as e:
                # Unexpected error
                error_category = categorize_error(e, "unexpected")
                error_msg = f"Unexpected error in step {step_name}: {str(e)}"
                display.set_error(error_msg)
                
                # Update database status
                on_failure_status = step_config.get('on_failure_status')
                if on_failure_status and self.db_manager:
                    self.db_manager.update_case_status(case_id, on_failure_status, str(e))
                
                logger.error_with_exception(
                    f"Unexpected error in step {step_name}",
                    e,
                    context=LogContext(
                        case_id=str(case_id),
                        operation="step_execution",
                        error_category=error_category,
                        extra_data={"step_name": step_name}
                    ).to_dict()
                )
                
                return False
        
        # This should never be reached
        return False

    def find_task_by_label(self, label: str) -> tuple[str, Optional[Dict]]:
        """
        Find pueue task by label.
        
        Args:
            label: Label to search for in pueue tasks
            
        Returns:
            Tuple of (status, task_dict) where status is "found" or "not_found"
        """
        ssh_command = [
            self.ssh_cmd,
            f"{self.user}@{self.host}",
            self.pueue_cmd,
            "status",
            "--json",
        ]
        
        try:
            result = subprocess.run(
                ssh_command, check=True, capture_output=True, text=True, timeout=60
            )
            status_data = json.loads(result.stdout)
            tasks = status_data.get("tasks", {})
            
            # Search through tasks for matching label
            for task_id, task_info in tasks.items():
                if task_info.get("label") == label:
                    return ("found", {"id": int(task_id), **task_info})
            
            return ("not_found", None)
            
        except (
            subprocess.TimeoutExpired,
            subprocess.CalledProcessError,
            json.JSONDecodeError,
            KeyError,
        ) as e:
            logger.warning_with_exception(
                "Failed to search tasks by label on HPC",
                e,
                context=LogContext(
                    operation="task_label_search",
                    extra_data={"label": label}
                ).to_dict()
            )
            return ("not_found", None)

    def kill_workflow(self, task_id: str) -> bool:
        """
        Kill a running workflow by task ID.
        
        Args:
            task_id: ID of the pueue task to kill
            
        Returns:
            True if successfully killed, False otherwise
        """
        ssh_command = [
            self.ssh_cmd,
            f"{self.user}@{self.host}",
            self.pueue_cmd,
            "kill",
            str(task_id)
        ]
        
        try:
            result = subprocess.run(
                ssh_command, check=True, capture_output=True, text=True, timeout=60
            )
            
            logger.info(
                f"Successfully killed pueue task {task_id}",
                context=LogContext(
                    operation="kill_workflow",
                    extra_data={"task_id": task_id}
                ).to_dict()
            )
            return True
            
        except (subprocess.TimeoutExpired, subprocess.CalledProcessError) as e:
            logger.warning_with_exception(
                f"Failed to kill pueue task {task_id}",
                e,
                context=LogContext(
                    operation="kill_workflow",
                    extra_data={"task_id": task_id}
                ).to_dict()
            )
            return False

    def get_workflow_status(self, task_id: str) -> str:
        """
        Get status of workflow task.
        
        Args:
            task_id: ID of the pueue task to check
            
        Returns:
            String representing the task status
        """
        return self.remote_executor.get_workflow_status(int(task_id))



