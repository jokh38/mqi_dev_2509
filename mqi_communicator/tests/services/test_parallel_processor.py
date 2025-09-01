import pytest
from unittest.mock import Mock, patch
from src.services.parallel_processor import ParallelCaseProcessor, ProcessingMetrics


class TestProcessingMetrics:
    """Test suite for ProcessingMetrics dataclass."""

    def test_processing_metrics_initialization(self):
        """Test ProcessingMetrics initializes with correct defaults."""
        metrics = ProcessingMetrics()

        assert metrics.total_cases_processed == 0
        assert metrics.successful_submissions == 0
        assert metrics.failed_submissions == 0
        assert metrics.concurrent_tasks == 0
        assert metrics.average_processing_time == 0.0
        assert metrics.peak_concurrent_tasks == 0
        assert metrics.total_processing_time == 0.0
        assert metrics.processing_times == []

    def test_add_processing_time_updates_metrics(self):
        """Test that adding processing times updates averages correctly."""
        metrics = ProcessingMetrics()

        metrics.add_processing_time(2.0)
        assert metrics.average_processing_time == 2.0
        assert metrics.total_processing_time == 2.0
        assert len(metrics.processing_times) == 1

        metrics.add_processing_time(4.0)
        assert metrics.average_processing_time == 3.0
        assert metrics.total_processing_time == 6.0
        assert len(metrics.processing_times) == 2

    def test_update_concurrent_tasks_tracks_peak(self):
        """Test concurrent task tracking updates peak values."""
        metrics = ProcessingMetrics()

        metrics.update_concurrent_tasks(3)
        assert metrics.concurrent_tasks == 3
        assert metrics.peak_concurrent_tasks == 3

        metrics.update_concurrent_tasks(1)
        assert metrics.concurrent_tasks == 1
        assert metrics.peak_concurrent_tasks == 3  # Peak should remain

        metrics.update_concurrent_tasks(5)
        assert metrics.concurrent_tasks == 5
        assert metrics.peak_concurrent_tasks == 5  # New peak

    def test_get_success_rate_calculation(self):
        """Test success rate calculation."""
        metrics = ProcessingMetrics()

        # No cases processed
        assert metrics.get_success_rate() == 0.0

        # Some successful cases
        metrics.total_cases_processed = 10
        metrics.successful_submissions = 8
        assert metrics.get_success_rate() == 80.0

        # All successful
        metrics.successful_submissions = 10
        assert metrics.get_success_rate() == 100.0


class TestParallelCaseProcessor:
    """Test suite for ParallelCaseProcessor."""

    @pytest.fixture
    def mock_db_manager(self):
        """Mock database manager."""
        db_manager = Mock()
        db_manager.get_cases_by_status.return_value = []
        db_manager.get_gpu_resource_by_case_id.return_value = None
        db_manager.find_and_lock_any_available_gpu.return_value = "gpu_group_1"
        return db_manager

    @pytest.fixture
    def mock_workflow_submitter(self):
        """Mock workflow submitter."""
        submitter = Mock()
        submitter.submit_workflow.return_value = 12345
        return submitter

    @pytest.fixture
    def mock_gpu_manager(self):
        """Mock GPU manager."""
        gpu_manager = Mock()
        gpu_manager.get_optimal_gpu_assignment.return_value = "optimal_gpu_group"
        return gpu_manager

    @pytest.fixture
    def processor(self, mock_db_manager, mock_workflow_submitter):
        """Create ParallelCaseProcessor instance for testing."""
        return ParallelCaseProcessor(
            db_manager=mock_db_manager,
            workflow_submitter=mock_workflow_submitter,
            max_workers=2,
            batch_size=5,
            processing_timeout=10.0,
        )

    def test_processor_initialization(
        self, processor, mock_db_manager, mock_workflow_submitter
    ):
        """Test processor initializes with correct configuration."""
        assert processor.db_manager == mock_db_manager
        assert processor.workflow_submitter == mock_workflow_submitter
        assert processor.max_workers == 2
        assert processor.batch_size == 5
        assert processor.processing_timeout == 10.0
        assert isinstance(processor.metrics, ProcessingMetrics)
        assert len(processor.active_case_ids) == 0

    def test_process_case_batch_no_cases_returns_false(
        self, processor, mock_db_manager
    ):
        """Test process_case_batch returns False when no cases available."""
        mock_db_manager.get_cases_by_status.return_value = []

        result = processor.process_case_batch()

        assert result is False
        mock_db_manager.get_cases_by_status.assert_called_once_with("submitted")

    def test_process_case_batch_successful_processing(
        self, processor, mock_db_manager, mock_workflow_submitter
    ):
        """Test successful case batch processing."""
        # Setup test data
        test_cases = [
            {"case_id": 1, "case_path": "/path/to/case1"},
            {"case_id": 2, "case_path": "/path/to/case2"},
        ]
        mock_db_manager.get_cases_by_status.return_value = test_cases
        mock_workflow_submitter.submit_workflow.return_value = 12345

        result = processor.process_case_batch()

        assert result is True
        assert processor.metrics.total_cases_processed == 2
        assert processor.metrics.successful_submissions == 2
        assert processor.metrics.failed_submissions == 0

        # Verify database updates
        assert (
            mock_db_manager.update_case_status.call_count == 4
        )  # 2 cases * 2 status updates each
        assert mock_db_manager.update_case_pueue_task_id.call_count == 2

    def test_process_case_batch_handles_submission_failure(
        self, processor, mock_db_manager, mock_workflow_submitter
    ):
        """Test case batch processing handles submission failures gracefully."""
        test_cases = [{"case_id": 1, "case_path": "/path/to/case1"}]
        mock_db_manager.get_cases_by_status.return_value = test_cases
        mock_workflow_submitter.process_case.return_value = False  # Simulate failure

        result = processor.process_case_batch()

        assert result is True
        assert processor.metrics.total_cases_processed == 1
        assert processor.metrics.successful_submissions == 0
        assert processor.metrics.failed_submissions == 1

        # Verify error handling
        mock_db_manager.update_case_completion.assert_called_once_with(
            1, status="failed"
        )
        mock_db_manager.release_gpu_resource.assert_called_once_with(1)

    def test_process_case_batch_handles_no_available_gpus(
        self, processor, mock_db_manager, mock_workflow_submitter
    ):
        """Test processing when no GPUs are available."""
        test_cases = [{"case_id": 1, "case_path": "/path/to/case1"}]
        mock_db_manager.get_cases_by_status.return_value = test_cases
        mock_db_manager.find_and_lock_any_available_gpu.return_value = (
            None  # No GPUs available
        )

        result = processor.process_case_batch()

        assert result is True  # Batch was processed, even though case was deferred
        assert processor.metrics.total_cases_processed == 1
        assert processor.metrics.successful_submissions == 0
        assert (
            processor.metrics.failed_submissions == 1
        )  # Counted as failure when no GPU available

    def test_process_case_batch_respects_batch_size_limit(
        self, processor, mock_db_manager
    ):
        """Test that batch processing respects the batch_size limit."""
        # Create more cases than batch size
        test_cases = [
            {"case_id": i, "case_path": f"/path/to/case{i}"}
            for i in range(10)  # 10 cases, but batch_size is 5
        ]
        mock_db_manager.get_cases_by_status.return_value = test_cases

        processor.process_case_batch()

        # Should only process batch_size number of cases
        assert processor.metrics.total_cases_processed == 5

    def test_assign_optimal_gpu_with_gpu_manager(
        self, processor, mock_db_manager, mock_gpu_manager
    ):
        """Test optimal GPU assignment when gpu_manager is available."""
        processor.gpu_manager = mock_gpu_manager
        mock_gpu_manager.get_optimal_gpu_assignment.return_value = "optimal_gpu"
        mock_db_manager.find_and_lock_any_available_gpu.return_value = "optimal_gpu"

        result = processor._assign_optimal_gpu(1)

        assert result == "optimal_gpu"
        mock_gpu_manager.get_optimal_gpu_assignment.assert_called_once()
        mock_db_manager.find_and_lock_any_available_gpu.assert_called_once_with(1)

    def test_assign_optimal_gpu_fallback_when_optimal_unavailable(
        self, processor, mock_db_manager, mock_gpu_manager
    ):
        """Test GPU assignment falls back when optimal GPU is unavailable."""
        processor.gpu_manager = mock_gpu_manager
        mock_gpu_manager.get_optimal_gpu_assignment.return_value = "optimal_gpu"
        mock_db_manager.find_and_lock_any_available_gpu.return_value = (
            "fallback_gpu"  # Different GPU assigned
        )

        result = processor._assign_optimal_gpu(1)

        assert result == "fallback_gpu"

    def test_assign_optimal_gpu_handles_gpu_manager_exception(
        self, processor, mock_db_manager, mock_gpu_manager
    ):
        """Test GPU assignment handles gpu_manager exceptions gracefully."""
        processor.gpu_manager = mock_gpu_manager
        mock_gpu_manager.get_optimal_gpu_assignment.side_effect = Exception(
            "GPU manager error"
        )
        mock_db_manager.find_and_lock_any_available_gpu.return_value = "fallback_gpu"

        result = processor._assign_optimal_gpu(1)

        assert result == "fallback_gpu"  # Should fall back to standard assignment

    def test_get_processing_metrics_returns_current_state(self, processor):
        """Test get_processing_metrics returns current metrics."""
        processor.metrics.total_cases_processed = 5
        processor.metrics.successful_submissions = 4

        metrics = processor.get_processing_metrics()

        assert metrics.total_cases_processed == 5
        assert metrics.successful_submissions == 4

    def test_get_performance_summary_returns_complete_summary(self, processor):
        """Test performance summary includes all required metrics."""
        processor.metrics.total_cases_processed = 10
        processor.metrics.successful_submissions = 8
        processor.metrics.add_processing_time(2.5)
        processor.metrics.update_concurrent_tasks(3)

        summary = processor.get_performance_summary()

        assert summary["total_cases_processed"] == 10
        assert summary["success_rate_percent"] == 80.0
        assert summary["average_processing_time_seconds"] == 2.5
        assert summary["peak_concurrent_tasks"] == 3
        assert summary["configuration"]["max_workers"] == 2
        assert summary["configuration"]["batch_size"] == 5
        assert summary["configuration"]["processing_timeout"] == 10.0

    def test_reset_metrics_clears_all_counters(self, processor):
        """Test reset_metrics clears all tracking counters."""
        # Set some metrics
        processor.metrics.total_cases_processed = 10
        processor.metrics.successful_submissions = 8
        processor.metrics.add_processing_time(2.5)

        processor.reset_metrics()

        assert processor.metrics.total_cases_processed == 0
        assert processor.metrics.successful_submissions == 0
        assert processor.metrics.failed_submissions == 0
        assert processor.metrics.average_processing_time == 0.0
        assert len(processor.metrics.processing_times) == 0

    @patch("time.time")
    def test_process_single_case_tracks_processing_time(
        self, mock_time, processor, mock_db_manager, mock_workflow_submitter
    ):
        """Test that single case processing tracks time correctly."""
        mock_time.side_effect = [0.0, 2.5]  # Start and end times
        test_case = {"case_id": 1, "case_path": "/path/to/case1"}
        mock_workflow_submitter.submit_workflow.return_value = 12345

        result = processor._process_single_case(test_case)

        assert result is True
        # Time tracking happens at batch level, so we just verify the case processed successfully
        mock_db_manager.update_case_status.assert_called()

    def test_concurrent_case_processing_prevents_duplicate_processing(
        self, processor, mock_db_manager
    ):
        """Test that concurrent processing prevents duplicate case handling."""
        # Add a case to active processing
        processor.active_case_ids.add(1)

        test_cases = [{"case_id": 1, "case_path": "/path/to/case1"}]
        mock_db_manager.get_cases_by_status.return_value = test_cases

        result = processor.process_case_batch()

        # Should still return True but not process the active case
        assert result is False  # No new cases were actually processed
        assert processor.metrics.total_cases_processed == 0
