import logging
from datetime import datetime
from enum import IntEnum
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, field

from src.common.db_manager import DatabaseManager
from src.common.structured_logging import get_structured_logger, LogContext


class CasePriority(IntEnum):
    """Case priority levels (higher value = higher priority)."""

    LOW = 1
    NORMAL = 2
    HIGH = 3
    URGENT = 4
    CRITICAL = 5


@dataclass
class PriorityConfig:
    """Configuration for priority-based scheduling algorithms."""

    algorithm: str = "weighted_fair"  # "weighted_fair", "strict_priority", "aging"
    aging_factor: float = 0.1  # Priority boost per hour of waiting
    starvation_threshold_hours: int = (
        24  # Hours after which low priority cases get boost
    )
    priority_weights: Dict[int, float] = field(
        default_factory=lambda: {
            CasePriority.LOW: 1.0,
            CasePriority.NORMAL: 2.0,
            CasePriority.HIGH: 4.0,
            CasePriority.URGENT: 8.0,
            CasePriority.CRITICAL: 16.0,
        }
    )


@dataclass
class SchedulingMetrics:
    """Metrics for priority scheduling performance tracking."""

    cases_scheduled_by_priority: Dict[int, int] = field(default_factory=dict)
    average_wait_time_by_priority: Dict[int, float] = field(default_factory=dict)
    starvation_prevented: int = 0
    total_scheduling_decisions: int = 0
    algorithm_switches: int = 0

    def record_case_scheduled(self, priority: int, wait_time: float) -> None:
        """Record a case being scheduled."""
        self.cases_scheduled_by_priority[priority] = (
            self.cases_scheduled_by_priority.get(priority, 0) + 1
        )

        # Update average wait time
        current_avg = self.average_wait_time_by_priority.get(priority, 0.0)
        current_count = self.cases_scheduled_by_priority[priority]
        new_avg = ((current_avg * (current_count - 1)) + wait_time) / current_count
        self.average_wait_time_by_priority[priority] = new_avg

        self.total_scheduling_decisions += 1


class PriorityScheduler:
    """
    Priority-based scheduler for intelligent case scheduling with multiple algorithms
    and resource-aware optimization.
    """

    def __init__(
        self, db_manager: DatabaseManager, config: Optional[PriorityConfig] = None
    ):
        """
        Initialize the priority scheduler.

        Args:
            db_manager: Database manager instance
            config: Priority configuration, uses defaults if None
        """
        self.db_manager = db_manager
        self.config = config or PriorityConfig()
        self.metrics = SchedulingMetrics()
        self.logger = get_structured_logger(__name__)

        # Ensure priority column exists in cases table
        self._ensure_priority_column()

        self.logger.info("PriorityScheduler initialized", LogContext(
            operation="init",
            extra_data={
                "algorithm": self.config.algorithm,
                "aging_factor": self.config.aging_factor
            }
        ))

    def _ensure_priority_column(self) -> None:
        """Ensure the priority column exists in the cases table."""
        try:
            # Check if priority column exists by trying to select from it
            self.db_manager.cursor.execute("SELECT priority FROM cases LIMIT 1")
        except Exception:
            # Column doesn't exist, add it with default NORMAL priority
            self.db_manager.cursor.execute(
                "ALTER TABLE cases ADD COLUMN priority INTEGER DEFAULT 2"
            )
            self.db_manager.connection.commit()
            self.logger.info("Added priority column to cases table", LogContext(
                operation="ensure_priority_column",
                extra_data={"default_priority": "NORMAL"}
            ))

    def set_case_priority(self, case_id: int, priority: CasePriority) -> bool:
        """
        Set the priority for a specific case.

        Args:
            case_id: Case ID to update
            priority: New priority level

        Returns:
            bool: True if update was successful, False otherwise
        """
        try:
            self.db_manager.cursor.execute(
                "UPDATE cases SET priority = ? WHERE case_id = ?",
                (int(priority), case_id),
            )
            self.db_manager.connection.commit()

            if self.db_manager.cursor.rowcount > 0:
                self.logger.info("Case priority updated", LogContext(
                    case_id=str(case_id),
                    operation="set_priority",
                    extra_data={"priority": priority.name}
                ))
                return True
            else:
                self.logger.warning("Case not found when setting priority", LogContext(
                    case_id=str(case_id),
                    operation="set_priority"
                ))
                return False

        except Exception as e:
            self.logger.error_with_exception("Failed to set case priority", e, LogContext(
                case_id=str(case_id),
                operation="set_priority"
            ))
            return False

    def get_prioritized_cases(
        self, status: str = "submitted", limit: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """
        Get cases ordered by priority using the configured scheduling algorithm.

        Args:
            status: Case status to filter by
            limit: Maximum number of cases to return

        Returns:
            List of cases ordered by scheduling priority
        """
        try:
            if self.config.algorithm == "strict_priority":
                return self._get_cases_strict_priority(status, limit)
            elif self.config.algorithm == "aging":
                return self._get_cases_with_aging(status, limit)
            else:  # Default to weighted_fair
                return self._get_cases_weighted_fair(status, limit)
        except Exception as e:
            self.logger.error_with_exception("Failed to get prioritized cases", e, LogContext(
                operation="get_prioritized_cases",
                extra_data={"algorithm": self.config.algorithm, "status": status}
            ))
            # Fallback to basic priority ordering
            return self._get_cases_basic_priority(status, limit)

    def _get_cases_strict_priority(
        self, status: str, limit: Optional[int]
    ) -> List[Dict[str, Any]]:
        """Get cases using strict priority ordering (highest priority first)."""
        query = """
        SELECT * FROM cases 
        WHERE status = ? 
        ORDER BY priority DESC, created_at ASC
        """
        params = [status]

        if limit:
            query += " LIMIT ?"
            params.append(limit)

        self.db_manager.cursor.execute(query, params)
        cases = [dict(row) for row in self.db_manager.cursor.fetchall()]

        self.logger.debug("Retrieved cases using strict priority algorithm", LogContext(
            operation="get_cases_strict_priority",
            extra_data={"case_count": len(cases), "status": status}
        ))
        return cases

    def _get_cases_with_aging(
        self, status: str, limit: Optional[int]
    ) -> List[Dict[str, Any]]:
        """Get cases using aging algorithm (priority increases with wait time)."""
        current_time = datetime.now()

        # Get all cases and calculate aged priority
        query = "SELECT * FROM cases WHERE status = ? ORDER BY created_at ASC"
        self.db_manager.cursor.execute(query, (status,))
        cases = [dict(row) for row in self.db_manager.cursor.fetchall()]

        aged_cases = []
        for case in cases:
            # Calculate wait time in hours
            created_at = datetime.fromisoformat(case["created_at"])
            wait_hours = (current_time - created_at).total_seconds() / 3600.0

            # Calculate aged priority
            base_priority = case.get("priority", CasePriority.NORMAL)
            aged_priority = base_priority + (wait_hours * self.config.aging_factor)

            # Check for starvation prevention
            if (
                wait_hours > self.config.starvation_threshold_hours
                and base_priority <= CasePriority.NORMAL
            ):
                aged_priority += 2.0  # Significant boost for starved cases
                self.metrics.starvation_prevented += 1
                self.logger.info("Starvation prevention applied", LogContext(
                    case_id=str(case['case_id']),
                    operation="aging_algorithm",
                    extra_data={
                        "wait_hours": round(wait_hours, 1),
                        "base_priority": base_priority
                    }
                ))

            case["aged_priority"] = aged_priority
            aged_cases.append(case)

        # Sort by aged priority (descending) then by creation time (ascending)
        aged_cases.sort(key=lambda x: (-x["aged_priority"], x["created_at"]))

        if limit:
            aged_cases = aged_cases[:limit]

        self.logger.debug("Retrieved cases using aging algorithm", LogContext(
            operation="get_cases_with_aging",
            extra_data={"case_count": len(aged_cases), "status": status}
        ))
        return aged_cases

    def _get_cases_weighted_fair(
        self, status: str, limit: Optional[int]
    ) -> List[Dict[str, Any]]:
        """Get cases using weighted fair queuing algorithm."""
        current_time = datetime.now()

        # Get all cases and calculate weighted priority
        query = "SELECT * FROM cases WHERE status = ? ORDER BY created_at ASC"
        self.db_manager.cursor.execute(query, (status,))
        cases = [dict(row) for row in self.db_manager.cursor.fetchall()]

        weighted_cases = []
        for case in cases:
            # Calculate wait time in hours
            created_at = datetime.fromisoformat(case["created_at"])
            wait_hours = (current_time - created_at).total_seconds() / 3600.0

            # Get priority weight
            base_priority = case.get("priority", CasePriority.NORMAL)
            priority_weight = self.config.priority_weights.get(base_priority, 1.0)

            # Calculate weighted score (combines priority weight and wait time)
            weighted_score = priority_weight * (
                1.0 + (wait_hours * 0.05)
            )  # 5% boost per hour

            # Apply starvation prevention
            if (
                wait_hours > self.config.starvation_threshold_hours
                and base_priority <= CasePriority.NORMAL
            ):
                weighted_score *= 2.0  # Double the weight for starved cases
                self.metrics.starvation_prevented += 1
                self.logger.info("Starvation prevention applied", LogContext(
                    case_id=str(case['case_id']),
                    operation="weighted_fair_algorithm",
                    extra_data={"wait_hours": round(wait_hours, 1)}
                ))

            case["weighted_score"] = weighted_score
            weighted_cases.append(case)

        # Sort by weighted score (descending) then by creation time (ascending)
        weighted_cases.sort(key=lambda x: (-x["weighted_score"], x["created_at"]))

        if limit:
            weighted_cases = weighted_cases[:limit]

        self.logger.debug("Retrieved cases using weighted fair algorithm", LogContext(
            operation="get_cases_weighted_fair",
            extra_data={"case_count": len(weighted_cases), "status": status}
        ))
        return weighted_cases

    def _get_cases_basic_priority(
        self, status: str, limit: Optional[int]
    ) -> List[Dict[str, Any]]:
        """Fallback method for basic priority ordering."""
        query = """
        SELECT * FROM cases 
        WHERE status = ? 
        ORDER BY COALESCE(priority, 2) DESC, created_at ASC
        """
        params = [status]

        if limit:
            query += " LIMIT ?"
            params.append(limit)

        self.db_manager.cursor.execute(query, params)
        cases = [dict(row) for row in self.db_manager.cursor.fetchall()]

        self.logger.debug("Retrieved cases using basic priority fallback", LogContext(
            operation="get_cases_basic_priority",
            extra_data={"case_count": len(cases), "status": status}
        ))
        return cases

    def schedule_next_cases(self, available_gpus: int) -> List[Dict[str, Any]]:
        """
        Schedule the next cases to be processed based on available GPU resources.

        Args:
            available_gpus: Number of available GPU resources

        Returns:
            List of cases to be processed, ordered by priority
        """
        if available_gpus <= 0:
            return []

        # Get prioritized cases up to the number of available GPUs
        prioritized_cases = self.get_prioritized_cases(
            "submitted", limit=available_gpus
        )

        # Record scheduling metrics
        current_time = datetime.now()
        for case in prioritized_cases:
            created_at = datetime.fromisoformat(case["created_at"])
            wait_time = (current_time - created_at).total_seconds() / 3600.0  # Hours
            priority = case.get("priority", CasePriority.NORMAL)
            self.metrics.record_case_scheduled(priority, wait_time)

        if prioritized_cases:
            priorities = [
                case.get("priority", CasePriority.NORMAL) for case in prioritized_cases
            ]
            self.logger.info("Cases scheduled for processing", LogContext(
                operation="schedule_next_cases",
                extra_data={
                    "scheduled_count": len(prioritized_cases),
                    "available_gpus": available_gpus,
                    "priorities": priorities
                }
            ))

        return prioritized_cases

    def get_priority_statistics(self) -> Dict[str, Any]:
        """
        Get priority scheduling statistics and performance metrics.

        Returns:
            Dictionary with priority statistics
        """
        total_scheduled = sum(self.metrics.cases_scheduled_by_priority.values())

        statistics = {
            "algorithm": self.config.algorithm,
            "total_cases_scheduled": total_scheduled,
            "starvation_prevented": self.metrics.starvation_prevented,
            "scheduling_decisions": self.metrics.total_scheduling_decisions,
            "cases_by_priority": dict(self.metrics.cases_scheduled_by_priority),
            "average_wait_times": dict(self.metrics.average_wait_time_by_priority),
            "configuration": {
                "aging_factor": self.config.aging_factor,
                "starvation_threshold_hours": self.config.starvation_threshold_hours,
                "priority_weights": dict(self.config.priority_weights),
            },
        }

        # Calculate priority distribution percentages
        if total_scheduled > 0:
            priority_percentages = {}
            for priority, count in self.metrics.cases_scheduled_by_priority.items():
                priority_percentages[priority] = (count / total_scheduled) * 100
            statistics["priority_distribution_percent"] = priority_percentages

        return statistics

    def update_algorithm(self, algorithm: str) -> bool:
        """
        Update the scheduling algorithm at runtime.

        Args:
            algorithm: New algorithm ("strict_priority", "aging", "weighted_fair")

        Returns:
            bool: True if update was successful, False otherwise
        """
        valid_algorithms = ["strict_priority", "aging", "weighted_fair"]

        if algorithm not in valid_algorithms:
            self.logger.error("Invalid scheduling algorithm specified", LogContext(
                operation="update_algorithm",
                extra_data={
                    "invalid_algorithm": algorithm,
                    "valid_options": valid_algorithms
                }
            ))
            return False

        old_algorithm = self.config.algorithm
        self.config.algorithm = algorithm
        self.metrics.algorithm_switches += 1

        self.logger.info("Scheduling algorithm updated", LogContext(
            operation="update_algorithm",
            extra_data={
                "old_algorithm": old_algorithm,
                "new_algorithm": algorithm
            }
        ))
        return True

    def reset_metrics(self) -> None:
        """Reset scheduling metrics for new measurement period."""
        self.metrics = SchedulingMetrics()
        self.logger.info("Priority scheduling metrics reset", LogContext(
            operation="reset_metrics"
        ))
