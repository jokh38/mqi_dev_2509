import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, List, Optional, Set
from dataclasses import dataclass, field

from src.common.db_manager import DatabaseManager
from src.services.workflow_engine import WorkflowEngine
from src.common.structured_logging import get_structured_logger, LogContext


@dataclass
class ProcessingMetrics:
    """Metrics for parallel processing performance tracking."""

    total_cases_processed: int = 0
    successful_submissions: int = 0
    failed_submissions: int = 0
    concurrent_tasks: int = 0
    average_processing_time: float = 0.0
    peak_concurrent_tasks: int = 0
    total_processing_time: float = 0.0
    processing_times: List[float] = field(default_factory=list)

    def add_processing_time(self, processing_time: float) -> None:
        """Add a processing time and update average."""
        self.processing_times.append(processing_time)
        self.total_processing_time += processing_time
        self.average_processing_time = self.total_processing_time / len(
            self.processing_times
        )

    def update_concurrent_tasks(self, current_tasks: int) -> None:
        """Update concurrent task metrics."""
        self.concurrent_tasks = current_tasks
        if current_tasks > self.peak_concurrent_tasks:
            self.peak_concurrent_tasks = current_tasks

    def get_success_rate(self) -> float:
        """Calculate success rate percentage."""
        if self.total_cases_processed == 0:
            return 0.0
        return (self.successful_submissions / self.total_cases_processed) * 100


class ParallelCaseProcessor:
    """
    Parallel case processor that handles multiple case submissions concurrently
    using ThreadPoolExecutor for optimal resource utilization with priority scheduling.
    """

    def __init__(
        self,
        db_manager: DatabaseManager,
        workflow_engine: WorkflowEngine,
        gpu_manager: Optional[Any] = None,
        priority_scheduler: Optional[Any] = None,
        max_workers: int = 4,
        batch_size: int = 10,
        processing_timeout: float = 300.0,
    ):
        """
        Initialize the parallel case processor.

        Args:
            db_manager: Database manager instance
            workflow_engine: Workflow submitter instance
            gpu_manager: Optional DynamicGpuManager for optimal GPU assignment
            priority_scheduler: Optional PriorityScheduler for intelligent case ordering
            max_workers: Maximum number of concurrent processing threads
            batch_size: Maximum number of cases to process in one batch
            processing_timeout: Timeout in seconds for individual case processing
        """
        self.db_manager = db_manager
        self.workflow_engine = workflow_engine
        self.gpu_manager = gpu_manager
        self.priority_scheduler = priority_scheduler
        self.max_workers = max_workers
        self.batch_size = batch_size
        self.processing_timeout = processing_timeout

        self.metrics = ProcessingMetrics()
        self.active_case_ids: Set[int] = set()
        self.processing_lock = threading.Lock()
        self.logger = get_structured_logger(__name__)

        self.logger.info("ParallelCaseProcessor initialized", LogContext(
            operation="init",
            extra_data={
                "max_workers": max_workers,
                "batch_size": batch_size,
                "timeout_seconds": processing_timeout,
                "priority_scheduler_enabled": priority_scheduler is not None,
                "gpu_manager_enabled": gpu_manager is not None
            }
        ))

    def process_case_batch(self) -> bool:
        """
        Process a batch of submitted cases in parallel with priority scheduling.

        Returns:
            bool: True if any cases were processed, False otherwise
        """
        # Get cases using priority scheduler if available, otherwise use standard database query
        if self.priority_scheduler:
            submitted_cases = self.priority_scheduler.get_prioritized_cases(
                "submitted", limit=self.batch_size
            )
        else:
            submitted_cases = self.db_manager.get_cases_by_status("submitted")
            # Limit batch size to prevent resource exhaustion
            submitted_cases = submitted_cases[: self.batch_size]

        if not submitted_cases:
            return False

        cases_to_process = submitted_cases
        self.logger.info("Starting parallel batch processing", LogContext(
            operation="process_batch",
            extra_data={
                "case_count": len(cases_to_process),
                "priority_scheduler_used": self.priority_scheduler is not None,
                "max_workers": self.max_workers
            }
        ))

        batch_start_time = time.time()
        processed_count = 0

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # Submit all cases for parallel processing
            future_to_case = {}
            for case in cases_to_process:
                case_id = case["case_id"]

                # Skip if case is already being processed
                with self.processing_lock:
                    if case_id in self.active_case_ids:
                        self.logger.debug("Case already being processed", LogContext(
                            case_id=str(case_id),
                            operation="process_batch"
                        ))
                        continue
                    self.active_case_ids.add(case_id)

                future = executor.submit(self._process_single_case, case)
                future_to_case[future] = case

            # Update concurrent task metrics
            self.metrics.update_concurrent_tasks(len(future_to_case))

            # Process completed futures as they finish
            for future in as_completed(future_to_case, timeout=self.processing_timeout):
                case = future_to_case[future]
                case_id = case["case_id"]

                try:
                    success = future.result()
                    processed_count += 1

                    if success:
                        self.metrics.successful_submissions += 1
                        self.logger.info("Case processing completed successfully", LogContext(
                            case_id=str(case_id),
                            operation="process_batch"
                        ))
                    else:
                        self.metrics.failed_submissions += 1
                        self.logger.warning("Case processing failed", LogContext(
                            case_id=str(case_id),
                            operation="process_batch"
                        ))

                except Exception as e:
                    processed_count += 1
                    self.metrics.failed_submissions += 1
                    self.logger.error_with_exception("Exception during case processing", e, LogContext(
                        case_id=str(case_id),
                        operation="process_batch"
                    ))

                finally:
                    # Remove from active cases
                    with self.processing_lock:
                        self.active_case_ids.discard(case_id)

        # Update processing metrics
        batch_processing_time = time.time() - batch_start_time
        self.metrics.add_processing_time(batch_processing_time)
        self.metrics.total_cases_processed += processed_count

        avg_time_per_case = batch_processing_time / max(1, processed_count)
        self.logger.info("Parallel batch processing completed", LogContext(
            operation="process_batch",
            extra_data={
                "processed_count": processed_count,
                "total_time_seconds": round(batch_processing_time, 2),
                "avg_time_per_case_seconds": round(avg_time_per_case, 2)
            }
        ))

        return processed_count > 0

    def _process_single_case(self, case: Dict[str, Any]) -> bool:
        """
        Process a single case with optimal GPU assignment and error handling.

        Args:
            case: Case dictionary from database

        Returns:
            bool: True if processing was successful, False otherwise
        """
        case_id = case["case_id"]
        case_start_time = time.time()

        try:
            # Try optimal GPU assignment first if gpu_manager is available
            group_name = self._assign_optimal_gpu(case_id)

            if not group_name:
                self.logger.info("No available GPUs for case - deferring processing", LogContext(
                    case_id=str(case_id),
                    operation="process_single_case"
                ))
                return False

            # Update case status to submitting
            self.db_manager.update_case_pueue_group(case_id, group_name)
            self.db_manager.update_case_status(
                case_id, status="submitting", progress=10
            )

            # Process case through workflow engine
            success = self.workflow_engine.process_case(
                case_id=case_id,
                case_path=case["case_path"],
                pueue_group=group_name,
            )

            if success:
                # Successfully processed
                self.db_manager.update_case_status(
                    case_id, status="completed", progress=100
                )
                self.db_manager.release_gpu_resource(case_id)

                case_processing_time = time.time() - case_start_time
                self.logger.info("Case successfully processed", LogContext(
                    case_id=str(case_id),
                    gpu_group=group_name,
                    operation="process_single_case",
                    extra_data={"processing_time_seconds": round(case_processing_time, 2)}
                ))
                return True
            else:
                raise ValueError("Workflow processing failed.")

        except Exception as e:
            self.logger.error_with_exception("Failed to process case", e, LogContext(
                case_id=str(case_id),
                operation="process_single_case"
            ))
            self.db_manager.update_case_completion(case_id, status="failed")
            self.db_manager.release_gpu_resource(case_id)
            return False

    def _assign_optimal_gpu(self, case_id: int) -> Optional[str]:
        """
        Assign optimal GPU resource to a case.

        Args:
            case_id: Case ID to assign GPU to

        Returns:
            str: GPU group name if assignment successful, None otherwise
        """
        # Try optimal GPU assignment first if gpu_manager is available
        if self.gpu_manager:
            try:
                optimal_group = self.gpu_manager.get_optimal_gpu_assignment()
                if optimal_group:
                    # Try to lock the optimal resource
                    locked_resource = self.db_manager.find_and_lock_any_available_gpu(
                        case_id
                    )
                    if locked_resource == optimal_group:
                        self.logger.info("Optimal GPU resource assigned to case", LogContext(
                            case_id=str(case_id),
                            gpu_group=optimal_group,
                            operation="assign_gpu"
                        ))
                        return optimal_group
                    elif locked_resource:
                        group_name = (
                            locked_resource
                            if isinstance(locked_resource, str)
                            else locked_resource["pueue_group"]
                        )
                        self.logger.info("GPU resource assigned to case", LogContext(
                            case_id=str(case_id),
                            gpu_group=group_name,
                            operation="assign_gpu",
                            extra_data={"optimal_was": optimal_group}
                        ))
                        return group_name
            except Exception as e:
                self.logger.warning_with_exception("Optimal GPU assignment failed", e, LogContext(
                    case_id=str(case_id),
                    operation="assign_gpu"
                ))

        # Fallback to standard GPU assignment
        locked_pueue_group = self.db_manager.get_gpu_resource_by_case_id(
            case_id
        ) or self.db_manager.find_and_lock_any_available_gpu(case_id)

        if not locked_pueue_group:
            return None

        group_name = (
            locked_pueue_group
            if isinstance(locked_pueue_group, str)
            else locked_pueue_group["pueue_group"]
        )
        self.logger.info("GPU resource assigned to case via fallback", LogContext(
            case_id=str(case_id),
            gpu_group=group_name,
            operation="assign_gpu"
        ))
        return group_name

    def get_processing_metrics(self) -> ProcessingMetrics:
        """
        Get current processing metrics.

        Returns:
            ProcessingMetrics: Current metrics snapshot
        """
        return self.metrics

    def get_performance_summary(self) -> Dict[str, Any]:
        """
        Get performance summary for monitoring and reporting.

        Returns:
            Dict: Performance summary with key metrics
        """
        return {
            "total_cases_processed": self.metrics.total_cases_processed,
            "success_rate_percent": round(self.metrics.get_success_rate(), 2),
            "average_processing_time_seconds": round(
                self.metrics.average_processing_time, 2
            ),
            "peak_concurrent_tasks": self.metrics.peak_concurrent_tasks,
            "current_concurrent_tasks": self.metrics.concurrent_tasks,
            "successful_submissions": self.metrics.successful_submissions,
            "failed_submissions": self.metrics.failed_submissions,
            "configuration": {
                "max_workers": self.max_workers,
                "batch_size": self.batch_size,
                "processing_timeout": self.processing_timeout,
            },
        }

    def reset_metrics(self) -> None:
        """Reset processing metrics for new measurement period."""
        self.metrics = ProcessingMetrics()
        self.logger.info("Parallel processing metrics reset", LogContext(
            operation="reset_metrics"
        ))
