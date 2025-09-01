import pytest
import sqlite3
from datetime import datetime
from unittest.mock import Mock, patch
from src.services.priority_scheduler import PriorityScheduler, CasePriority, PriorityConfig, SchedulingMetrics


class TestCasePriority:
    """Test suite for CasePriority enum."""

    def test_priority_values_are_ordered(self):
        """Test that priority values are correctly ordered."""
        assert CasePriority.LOW < CasePriority.NORMAL
        assert CasePriority.NORMAL < CasePriority.HIGH
        assert CasePriority.HIGH < CasePriority.URGENT
        assert CasePriority.URGENT < CasePriority.CRITICAL

    def test_priority_integer_values(self):
        """Test that priority enum values are correct integers."""
        assert int(CasePriority.LOW) == 1
        assert int(CasePriority.NORMAL) == 2
        assert int(CasePriority.HIGH) == 3
        assert int(CasePriority.URGENT) == 4
        assert int(CasePriority.CRITICAL) == 5


class TestPriorityConfig:
    """Test suite for PriorityConfig dataclass."""

    def test_priority_config_defaults(self):
        """Test PriorityConfig initializes with correct defaults."""
        config = PriorityConfig()

        assert config.algorithm == "weighted_fair"
        assert config.aging_factor == 0.1
        assert config.starvation_threshold_hours == 24
        assert len(config.priority_weights) == 5
        assert config.priority_weights[CasePriority.LOW] == 1.0
        assert config.priority_weights[CasePriority.CRITICAL] == 16.0

    def test_priority_config_custom_values(self):
        """Test PriorityConfig with custom values."""
        custom_weights = {CasePriority.LOW: 0.5, CasePriority.HIGH: 10.0}
        config = PriorityConfig(
            algorithm="strict_priority",
            aging_factor=0.2,
            starvation_threshold_hours=12,
            priority_weights=custom_weights,
        )

        assert config.algorithm == "strict_priority"
        assert config.aging_factor == 0.2
        assert config.starvation_threshold_hours == 12
        assert config.priority_weights == custom_weights


class TestSchedulingMetrics:
    """Test suite for SchedulingMetrics dataclass."""

    def test_scheduling_metrics_initialization(self):
        """Test SchedulingMetrics initializes with correct defaults."""
        metrics = SchedulingMetrics()

        assert metrics.cases_scheduled_by_priority == {}
        assert metrics.average_wait_time_by_priority == {}
        assert metrics.starvation_prevented == 0
        assert metrics.total_scheduling_decisions == 0
        assert metrics.algorithm_switches == 0

    def test_record_case_scheduled_updates_metrics(self):
        """Test that recording scheduled cases updates metrics correctly."""
        metrics = SchedulingMetrics()

        # Record first case
        metrics.record_case_scheduled(CasePriority.HIGH, 5.0)

        assert metrics.cases_scheduled_by_priority[CasePriority.HIGH] == 1
        assert metrics.average_wait_time_by_priority[CasePriority.HIGH] == 5.0
        assert metrics.total_scheduling_decisions == 1

        # Record second case with same priority
        metrics.record_case_scheduled(CasePriority.HIGH, 3.0)

        assert metrics.cases_scheduled_by_priority[CasePriority.HIGH] == 2
        assert (
            metrics.average_wait_time_by_priority[CasePriority.HIGH] == 4.0
        )  # (5+3)/2
        assert metrics.total_scheduling_decisions == 2


class TestPriorityScheduler:
    """Test suite for PriorityScheduler."""

    @pytest.fixture
    def mock_db_manager(self):
        """Mock database manager with in-memory SQLite."""
        db_manager = Mock()

        # Create in-memory SQLite connection for testing
        connection = sqlite3.connect(":memory:")
        connection.row_factory = sqlite3.Row
        cursor = connection.cursor()

        # Create cases table
        cursor.execute(
            """
        CREATE TABLE cases (
            case_id INTEGER PRIMARY KEY,
            case_path TEXT,
            status TEXT,
            priority INTEGER DEFAULT 2,
            created_at TEXT,
            status_updated_at TEXT
        )
        """
        )

        db_manager.connection = connection
        db_manager.cursor = cursor

        return db_manager

    @pytest.fixture
    def scheduler(self, mock_db_manager):
        """Create PriorityScheduler instance for testing."""
        return PriorityScheduler(db_manager=mock_db_manager)

    def test_scheduler_initialization(self, scheduler, mock_db_manager):
        """Test scheduler initializes with correct configuration."""
        assert scheduler.db_manager == mock_db_manager
        assert isinstance(scheduler.config, PriorityConfig)
        assert isinstance(scheduler.metrics, SchedulingMetrics)
        assert scheduler.config.algorithm == "weighted_fair"

    def test_scheduler_with_custom_config(self, mock_db_manager):
        """Test scheduler initialization with custom configuration."""
        custom_config = PriorityConfig(algorithm="strict_priority", aging_factor=0.2)
        scheduler = PriorityScheduler(db_manager=mock_db_manager, config=custom_config)

        assert scheduler.config.algorithm == "strict_priority"
        assert scheduler.config.aging_factor == 0.2

    def test_set_case_priority_success(self, scheduler, mock_db_manager):
        """Test setting case priority successfully."""
        # Insert test case
        mock_db_manager.cursor.execute(
            "INSERT INTO cases (case_id, case_path, status) VALUES (1, '/test/path', 'submitted')"
        )

        result = scheduler.set_case_priority(1, CasePriority.HIGH)

        assert result is True

        # Verify priority was set
        mock_db_manager.cursor.execute("SELECT priority FROM cases WHERE case_id = 1")
        row = mock_db_manager.cursor.fetchone()
        assert row["priority"] == int(CasePriority.HIGH)

    def test_set_case_priority_case_not_found(self, scheduler, mock_db_manager):
        """Test setting priority for non-existent case."""
        # Try to set priority for non-existent case
        result = scheduler.set_case_priority(999, CasePriority.HIGH)

        # Should return False since no rows are affected (case doesn't exist)
        assert result is False

    def test_get_cases_strict_priority(self, scheduler, mock_db_manager):
        """Test getting cases using strict priority algorithm."""
        scheduler.config.algorithm = "strict_priority"

        # Insert test cases with different priorities
        test_cases = [
            (1, "submitted", CasePriority.LOW, "2023-01-01T10:00:00"),
            (2, "submitted", CasePriority.HIGH, "2023-01-01T11:00:00"),
            (3, "submitted", CasePriority.NORMAL, "2023-01-01T09:00:00"),
        ]

        for case_id, status, priority, created_at in test_cases:
            mock_db_manager.cursor.execute(
                "INSERT INTO cases (case_id, status, priority, created_at) VALUES (?, ?, ?, ?)",
                (case_id, status, int(priority), created_at),
            )

        cases = scheduler.get_prioritized_cases("submitted")

        # Should be ordered by priority (HIGH, NORMAL, LOW) then by creation time
        assert len(cases) == 3
        assert cases[0]["case_id"] == 2  # HIGH priority
        assert cases[1]["case_id"] == 3  # NORMAL priority
        assert cases[2]["case_id"] == 1  # LOW priority

    def test_get_cases_with_limit(self, scheduler, mock_db_manager):
        """Test getting cases with limit parameter."""
        # Insert multiple test cases
        for i in range(5):
            mock_db_manager.cursor.execute(
                "INSERT INTO cases (case_id, status, priority) VALUES (?, ?, ?)",
                (i + 1, "submitted", CasePriority.NORMAL),
            )

        cases = scheduler.get_prioritized_cases("submitted", limit=3)

        assert len(cases) == 3

    @patch("src.services.priority_scheduler.datetime")
    def test_get_cases_with_aging(self, mock_datetime, scheduler, mock_db_manager):
        """Test getting cases using aging algorithm."""
        scheduler.config.algorithm = "aging"

        # Mock current time
        current_time = datetime(2023, 1, 1, 12, 0, 0)
        mock_datetime.now.return_value = current_time
        mock_datetime.fromisoformat.side_effect = datetime.fromisoformat

        # Insert test cases with different ages
        test_cases = [
            (1, "submitted", CasePriority.LOW, "2023-01-01T08:00:00"),  # 4 hours old
            (2, "submitted", CasePriority.NORMAL, "2023-01-01T10:00:00"),  # 2 hours old
            (3, "submitted", CasePriority.HIGH, "2023-01-01T11:00:00"),  # 1 hour old
        ]

        for case_id, status, priority, created_at in test_cases:
            mock_db_manager.cursor.execute(
                "INSERT INTO cases (case_id, status, priority, created_at) VALUES (?, ?, ?, ?)",
                (case_id, status, int(priority), created_at),
            )

        cases = scheduler.get_prioritized_cases("submitted")

        # Should be ordered by aged priority (HIGH + 0.1 hrs, NORMAL + 0.2 hrs, LOW + 0.4 hrs)
        assert len(cases) == 3
        # Verify aging was applied
        for case in cases:
            assert "aged_priority" in case

    @patch("src.services.priority_scheduler.datetime")
    def test_starvation_prevention(self, mock_datetime, scheduler, mock_db_manager):
        """Test starvation prevention for old low-priority cases."""
        scheduler.config.algorithm = "aging"
        scheduler.config.starvation_threshold_hours = 2  # Lower threshold for testing

        # Mock current time
        current_time = datetime(2023, 1, 1, 12, 0, 0)
        mock_datetime.now.return_value = current_time
        mock_datetime.fromisoformat.side_effect = datetime.fromisoformat

        # Insert a low priority case that's older than starvation threshold
        mock_db_manager.cursor.execute(
            "INSERT INTO cases (case_id, status, priority, created_at) VALUES (?, ?, ?, ?)",
            (
                1,
                "submitted",
                int(CasePriority.LOW),
                "2023-01-01T08:00:00",
            ),  # 4 hours old
        )

        cases = scheduler.get_prioritized_cases("submitted")

        assert len(cases) == 1
        assert scheduler.metrics.starvation_prevented == 1

    def test_schedule_next_cases_with_available_gpus(self, scheduler, mock_db_manager):
        """Test scheduling next cases based on available GPU resources."""
        # Insert test cases
        for i in range(5):
            mock_db_manager.cursor.execute(
                "INSERT INTO cases (case_id, status, priority, created_at) VALUES (?, ?, ?, ?)",
                (i + 1, "submitted", CasePriority.NORMAL, "2023-01-01T10:00:00"),
            )

        # Schedule for 3 available GPUs
        scheduled_cases = scheduler.schedule_next_cases(available_gpus=3)

        assert len(scheduled_cases) == 3
        assert scheduler.metrics.total_scheduling_decisions == 3

    def test_schedule_next_cases_no_available_gpus(self, scheduler):
        """Test scheduling when no GPUs are available."""
        scheduled_cases = scheduler.schedule_next_cases(available_gpus=0)

        assert len(scheduled_cases) == 0
        assert scheduler.metrics.total_scheduling_decisions == 0

    def test_get_priority_statistics(self, scheduler):
        """Test getting priority statistics."""
        # Record some scheduling decisions
        scheduler.metrics.record_case_scheduled(CasePriority.HIGH, 2.0)
        scheduler.metrics.record_case_scheduled(CasePriority.NORMAL, 4.0)
        scheduler.metrics.starvation_prevented = 1

        stats = scheduler.get_priority_statistics()

        assert stats["algorithm"] == "weighted_fair"
        assert stats["total_cases_scheduled"] == 2
        assert stats["starvation_prevented"] == 1
        assert stats["cases_by_priority"][CasePriority.HIGH] == 1
        assert stats["cases_by_priority"][CasePriority.NORMAL] == 1
        assert "configuration" in stats
        assert "priority_distribution_percent" in stats

    def test_update_algorithm_valid(self, scheduler):
        """Test updating to a valid algorithm."""
        result = scheduler.update_algorithm("strict_priority")

        assert result is True
        assert scheduler.config.algorithm == "strict_priority"
        assert scheduler.metrics.algorithm_switches == 1

    def test_update_algorithm_invalid(self, scheduler):
        """Test updating to an invalid algorithm."""
        original_algorithm = scheduler.config.algorithm
        result = scheduler.update_algorithm("invalid_algorithm")

        assert result is False
        assert scheduler.config.algorithm == original_algorithm
        assert scheduler.metrics.algorithm_switches == 0

    def test_reset_metrics(self, scheduler):
        """Test resetting scheduler metrics."""
        # Set some metrics
        scheduler.metrics.record_case_scheduled(CasePriority.HIGH, 5.0)
        scheduler.metrics.starvation_prevented = 3

        scheduler.reset_metrics()

        assert scheduler.metrics.total_scheduling_decisions == 0
        assert scheduler.metrics.starvation_prevented == 0
        assert len(scheduler.metrics.cases_scheduled_by_priority) == 0

    def test_fallback_to_basic_priority(self, scheduler, mock_db_manager):
        """Test fallback to basic priority when advanced algorithms fail."""
        # Force an exception in the priority calculation
        with patch.object(
            scheduler, "_get_cases_weighted_fair", side_effect=Exception("Test error")
        ):
            # Insert test case
            mock_db_manager.cursor.execute(
                "INSERT INTO cases (case_id, status, priority) VALUES (1, 'submitted', 2)"
            )

            cases = scheduler.get_prioritized_cases("submitted")

            # Should fall back to basic priority and still return cases
            assert len(cases) == 1
            assert cases[0]["case_id"] == 1
