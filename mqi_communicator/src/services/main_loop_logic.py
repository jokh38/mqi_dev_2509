import logging
from datetime import datetime, timedelta
from typing import Any, Optional

# Note: To avoid circular imports, type hint the manager classes
# instead of importing them directly.
from src.common.db_manager import DatabaseManager
from src.services.workflow_engine import WorkflowEngine
from src.common.structured_logging import get_structured_logger, LogContext

# Initialize module-level logger
logger = get_structured_logger(__name__)


def recover_stuck_submitting_cases(
    db_manager: DatabaseManager, workflow_engine: WorkflowEngine
) -> None:
    """
    Finds cases stuck in the 'submitting' state and attempts to recover them.
    This can happen if the application crashes after a job has been submitted
    to the HPC but before the local database could be updated.
    """
    stuck_submitting_cases = db_manager.get_cases_by_status("submitting")
    if not stuck_submitting_cases:
        return

    logger.warning("Found stuck submitting cases - attempting recovery", LogContext(
        operation="recover_stuck_cases",
        extra_data={"stuck_count": len(stuck_submitting_cases)}
    ))
    for case in stuck_submitting_cases:
        case_id = case["case_id"]
        label = f"mqic_case_{case_id}"
        logger.info("Checking remote task status for stuck case", LogContext(
            case_id=str(case_id),
            operation="recover_stuck_cases",
            extra_data={"task_label": label}
        ))

        status, remote_task = workflow_engine.find_task_by_label(label)

        if status == "found":
            if remote_task and (task_id := remote_task.get("id")) is not None:
                logger.warning("Found orphaned remote task - recovering to running state", LogContext(
                    case_id=str(case_id),
                    task_id=task_id,
                    operation="recover_stuck_cases"
                ))
                db_manager.update_case_pueue_task_id(case_id, task_id)
                db_manager.update_case_status(case_id, status="running", progress=30)
            else:
                logger.error("Remote task has no ID - cannot recover", LogContext(
                    case_id=str(case_id),
                    operation="recover_stuck_cases"
                ))
                db_manager.update_case_completion(case_id, status="failed")
                db_manager.release_gpu_resource(case_id)
        elif status == "not_found":
            logger.warning("No remote task found - submission likely failed", LogContext(
                case_id=str(case_id),
                operation="recover_stuck_cases"
            ))
            db_manager.update_case_completion(case_id, status="failed")
            db_manager.release_gpu_resource(case_id)
        elif status == "unreachable":
            logger.warning("HPC unreachable - cannot check case status", LogContext(
                case_id=str(case_id),
                operation="recover_stuck_cases"
            ))


def manage_running_cases(
    db_manager: DatabaseManager,
    workflow_engine: WorkflowEngine,
    timeout_delta: timedelta,
    kst: Any,
) -> None:
    """
    Checks the status of all 'running' cases, handling timeouts, successes,
    and failures.
    """
    running_cases = db_manager.get_cases_by_status("running")
    if not running_cases:
        return

    logger.info("Found running cases to check status", LogContext(
        operation="manage_running_cases",
        extra_data={"running_count": len(running_cases)}
    ))
    for case in running_cases:
        case_id = case["case_id"]
        task_id = case["pueue_task_id"]
        status_updated_at = datetime.fromisoformat(case["status_updated_at"])

        if task_id is None:
            logger.critical("Case is running but has no task ID", LogContext(
                case_id=str(case_id),
                operation="manage_running_cases"
            ))
            db_manager.update_case_completion(case_id, status="failed")
            db_manager.release_gpu_resource(case_id)
            continue

        # Check for timeout
        if datetime.now(kst) - status_updated_at > timeout_delta:
            timeout_hours = timeout_delta.total_seconds() / 3600
            logger.critical("Case timed out - marking as failed", LogContext(
                case_id=str(case_id),
                task_id=task_id,
                operation="manage_running_cases",
                extra_data={"timeout_hours": timeout_hours}
            ))
            kill_successful = workflow_engine.kill_workflow(task_id)
            db_manager.update_case_completion(case_id, status="failed")

            if kill_successful:
                logger.info("Successfully killed timed-out task", LogContext(
                    case_id=str(case_id),
                    task_id=task_id,
                    operation="manage_running_cases"
                ))
                db_manager.release_gpu_resource(case_id)
            else:
                pueue_group = case["pueue_group"]
                logger.critical("Failed to kill timed-out task - marking resource as zombie", LogContext(
                    case_id=str(case_id),
                    task_id=task_id,
                    gpu_group=pueue_group,
                    operation="manage_running_cases"
                ))
                if pueue_group:
                    db_manager.update_gpu_status(
                        pueue_group, status="zombie", case_id=case_id
                    )
                else:
                    logger.critical("Timed-out case has no pueue_group - cannot mark as zombie", LogContext(
                        case_id=str(case_id),
                        task_id=task_id,
                        operation="manage_running_cases"
                    ))
            continue

        # Check remote status
        remote_status = workflow_engine.get_workflow_status(task_id)
        logger.info("Retrieved remote task status", LogContext(
            case_id=str(case_id),
            task_id=task_id,
            operation="manage_running_cases",
            extra_data={"remote_status": remote_status}
        ))

        if remote_status in ("success", "failure", "not_found"):
            db_manager.release_gpu_resource(case_id)
            final_status = "completed" if remote_status == "success" else "failed"
            db_manager.update_case_completion(case_id, status=final_status)
            if final_status == "completed":
                logger.info("Case completed successfully", LogContext(
                    case_id=str(case_id),
                    task_id=task_id,
                    operation="manage_running_cases"
                ))
            else:
                log_level = (
                    logging.WARNING if remote_status == "not_found" else logging.ERROR
                )
                logger.log(
                    log_level,
                    "Case finished with failure status",
                    LogContext(
                        case_id=str(case_id),
                        task_id=task_id,
                        operation="manage_running_cases",
                        extra_data={"remote_status": remote_status}
                    )
                )
        elif remote_status == "unreachable":
            logger.warning("HPC unreachable - cannot check case status", LogContext(
                case_id=str(case_id),
                task_id=task_id,
                operation="manage_running_cases"
            ))


def manage_zombie_resources(
    db_manager: DatabaseManager, workflow_engine: WorkflowEngine
) -> None:
    """
    Attempts to recover 'zombie' resources by killing the associated task.
    A resource becomes a zombie if its task timed out but could not be killed.
    """
    zombie_resources = db_manager.get_resources_by_status("zombie")
    if not zombie_resources:
        return

    logger.warning("Found zombie resources - attempting recovery", LogContext(
        operation="manage_zombie_resources",
        extra_data={"zombie_count": len(zombie_resources)}
    ))
    for resource in zombie_resources:
        case_id = resource["assigned_case_id"]
        pueue_group = resource["pueue_group"]
        zombie_case = db_manager.get_case_by_id(case_id)

        if not zombie_case or not (task_id := zombie_case.get("pueue_task_id")):
            logger.error("Cannot recover zombie resource - manual intervention required", LogContext(
                case_id=str(case_id),
                gpu_group=pueue_group,
                operation="manage_zombie_resources"
            ))
            continue

        logger.info("Attempting to kill zombie task", LogContext(
            case_id=str(case_id),
            task_id=task_id,
            gpu_group=pueue_group,
            operation="manage_zombie_resources"
        ))
        if workflow_engine.kill_workflow(task_id):
            logger.info("Successfully killed zombie task", LogContext(
                case_id=str(case_id),
                task_id=task_id,
                gpu_group=pueue_group,
                operation="manage_zombie_resources"
            ))
            db_manager.release_gpu_resource(case_id)
        else:
            logger.warning("Failed to kill zombie task - will retry", LogContext(
                case_id=str(case_id),
                task_id=task_id,
                gpu_group=pueue_group,
                operation="manage_zombie_resources"
            ))



def process_new_submitted_cases_with_optimization(
    db_manager: DatabaseManager,
    workflow_engine: WorkflowEngine,
    gpu_manager: Optional[Any] = None,
) -> None:
    """
    Enhanced version of process_new_submitted_cases that uses optimal GPU assignment.

    Processes new cases with 'submitted' status by assigning them to optimal
    GPU resources using the DynamicGpuManager when available, falling back
    to the original algorithm when not available.

    Args:
        db_manager: Database manager instance
        workflow_engine: Workflow submitter instance
        gpu_manager: Optional DynamicGpuManager instance for optimal assignment
    """
    submitted_cases = db_manager.get_cases_by_status("submitted")
    if not submitted_cases:
        return

    logger.info("Found submitted cases to process with optimization", LogContext(
        operation="process_submitted_cases_optimized",
        extra_data={"submitted_count": len(submitted_cases)}
    ))
    for case_to_process in submitted_cases:
        case_id = case_to_process["case_id"]

        # Try optimal GPU assignment first if gpu_manager is available
        group_name = None
        if gpu_manager:
            try:
                optimal_group = gpu_manager.get_optimal_gpu_assignment()
                if optimal_group:
                    # Lock the optimal resource
                    locked_resource = db_manager.find_and_lock_any_available_gpu(
                        case_id
                    )
                    # Check if we got the optimal one, or use what we got
                    if locked_resource == optimal_group:
                        group_name = optimal_group
                        logger.info("Optimal GPU resource assigned to case", LogContext(
                            case_id=str(case_id),
                            gpu_group=group_name,
                            operation="process_submitted_cases_optimized"
                        ))
                    elif locked_resource:
                        group_name = (
                            locked_resource
                            if isinstance(locked_resource, str)
                            else locked_resource["pueue_group"]
                        )
                        logger.info("GPU resource assigned to case", LogContext(
                            case_id=str(case_id),
                            gpu_group=group_name,
                            operation="process_submitted_cases_optimized",
                            extra_data={"optimal_was": optimal_group}
                        ))
            except Exception as e:
                logger.warning_with_exception("Optimal GPU assignment failed - using fallback", e, LogContext(
                    case_id=str(case_id),
                    operation="process_submitted_cases_optimized"
                ))

        # Fallback to original allocation if optimal assignment didn't work
        if not group_name:
            locked_pueue_group = db_manager.get_gpu_resource_by_case_id(
                case_id
            ) or db_manager.find_and_lock_any_available_gpu(case_id)

            if not locked_pueue_group:
                logger.info("No available GPUs - will retry next cycle", LogContext(
                    case_id=str(case_id),
                    operation="process_submitted_cases_optimized"
                ))
                break  # No need to check other cases if no GPUs are free

            group_name = (
                locked_pueue_group
                if isinstance(locked_pueue_group, str)
                else locked_pueue_group["pueue_group"]
            )
            logger.info("GPU resource locked for case via fallback", LogContext(
                case_id=str(case_id),
                gpu_group=group_name,
                operation="process_submitted_cases_optimized"
            ))

        # Process the case with the assigned GPU
        try:
            db_manager.update_case_pueue_group(case_id, group_name)
            db_manager.update_case_status(case_id, status="submitting", progress=10)

            success = workflow_engine.process_case(
                case_id=case_id,
                case_path=case_to_process["case_path"],
                pueue_group=group_name,
            )

            if success:
                db_manager.update_case_status(case_id, status="completed", progress=100)
                db_manager.release_gpu_resource(case_id)
                logger.info("Case successfully processed with optimization", LogContext(
                    case_id=str(case_id),
                    gpu_group=group_name,
                    operation="process_submitted_cases_optimized"
                ))
            else:
                raise ValueError("Workflow processing failed.")

        except Exception as e:
            logger.error_with_exception("Failed to process case with optimization", e, LogContext(
                case_id=str(case_id),
                operation="process_submitted_cases_optimized"
            ))
            db_manager.update_case_completion(case_id, status="failed")
            db_manager.release_gpu_resource(case_id)
            logger.info("Released GPU for failed case", LogContext(
                case_id=str(case_id),
                operation="process_submitted_cases_optimized"
            ))


def process_new_submitted_cases_parallel(
    db_manager: DatabaseManager,
    workflow_engine: WorkflowEngine,
    parallel_processor: Optional[Any] = None,
) -> bool:
    """
    Process submitted cases using parallel processing for improved performance.

    Args:
        db_manager: Database manager instance
        workflow_engine: Workflow submitter instance
        parallel_processor: ParallelCaseProcessor instance for concurrent processing

    Returns:
        bool: True if any cases were processed, False otherwise
    """
    if parallel_processor:
        try:
            return parallel_processor.process_case_batch()
        except Exception as e:
            logger.error_with_exception("Parallel processing failed - falling back to sequential", e, LogContext(
                operation="process_submitted_cases_parallel"
            ))

    # Fallback to sequential processing with optimization
    process_new_submitted_cases_with_optimization(db_manager, workflow_engine)
    return True
