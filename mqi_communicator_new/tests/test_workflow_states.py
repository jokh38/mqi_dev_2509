import unittest
from unittest.mock import MagicMock, patch
from pathlib import Path

# Adjust the import path
from mqi_communicator_new.src.states import (
    PreProcessingState,
    FileUploadState,
    HpcExecutionState,
    DownloadState,
    PostProcessingState,
)
from mqi_communicator_new.src.local_handler import ExecutionResult
from mqi_communicator_new.src.remote_handler import TransferResult

class TestWorkflowStates(unittest.TestCase):
    """
    Test cases for the workflow state classes.
    """

    def setUp(self):
        """
        Set up a mock context object for each test.
        """
        self.mock_context = MagicMock()
        self.mock_context.case_id = "test_case_001"
        self.mock_context.case_path = Path("/path/to/cases/test_case_001")

        # Mock handlers
        self.mock_context.local_handler = MagicMock()
        self.mock_context.remote_handler = MagicMock()

        # Mock other components
        self.mock_context.db_handler = MagicMock()
        self.mock_context.logger = MagicMock()
        self.mock_context.config = MagicMock()
        self.mock_context.send_status_update = MagicMock()

    # --- PreProcessingState Tests ---

    def test_preprocessing_state_success(self):
        """
        Test PreProcessingState successful execution.
        """
        state = PreProcessingState()
        self.mock_context.local_handler.execute_mqi_interpreter.return_value = ExecutionResult(success=True, output="", error="", return_code=0)

        next_state = state.execute(self.mock_context)

        self.mock_context.db_handler.record_workflow_step.assert_any_call("test_case_001", "preprocessing", "STARTED")
        self.mock_context.local_handler.execute_mqi_interpreter.assert_called_once()
        self.mock_context.db_handler.record_workflow_step.assert_any_call("test_case_001", "preprocessing", "COMPLETED")
        self.assertIsInstance(next_state, FileUploadState)

    def test_preprocessing_state_failure(self):
        """
        Test PreProcessingState on handler failure.
        """
        state = PreProcessingState()
        self.mock_context.local_handler.execute_mqi_interpreter.return_value = ExecutionResult(success=False, output="", error="P2 failed", return_code=1)

        next_state = state.execute(self.mock_context)

        self.mock_context.db_handler.record_workflow_step.assert_any_call("test_case_001", "preprocessing", "FAILED", "P2 failed")
        self.assertIsNone(next_state)

    # --- FileUploadState Tests ---

    def test_file_upload_state_success(self):
        """
        Test FileUploadState successful execution.
        """
        state = FileUploadState()
        self.mock_context.remote_handler.upload_files.return_value = TransferResult(success=True, message="OK", files_transferred=5)

        next_state = state.execute(self.mock_context)

        self.mock_context.remote_handler.upload_files.assert_called_once()
        self.mock_context.db_handler.record_workflow_step.assert_any_call("test_case_001", "file_upload", "COMPLETED")
        self.assertIsInstance(next_state, HpcExecutionState)

    def test_file_upload_state_failure(self):
        """
        Test FileUploadState on handler failure.
        """
        state = FileUploadState()
        self.mock_context.remote_handler.upload_files.return_value = TransferResult(success=False, message="SFTP failed", files_transferred=0)

        next_state = state.execute(self.mock_context)

        self.mock_context.db_handler.record_workflow_step.assert_any_call("test_case_001", "file_upload", "FAILED", "SFTP failed")
        self.assertIsNone(next_state)

    # --- HpcExecutionState Tests ---
    
    @patch('mqi_communicator_new.src.states.time.sleep', return_value=None) # Mock sleep to speed up test
    def test_hpc_execution_state_success(self, mock_sleep):
        """
        Test HpcExecutionState successful execution.
        """
        state = HpcExecutionState()
        # Simulate successful command execution to start the job
        self.mock_context.remote_handler.execute_remote_command.return_value = (True, "stdout", "")
        # Simulate job completion check
        self.mock_context.remote_handler.check_job_completion.return_value = True

        next_state = state.execute(self.mock_context)

        self.mock_context.remote_handler.execute_remote_command.assert_called_once()
        self.mock_context.remote_handler.check_job_completion.assert_called_once()
        self.mock_context.db_handler.record_workflow_step.assert_any_call("test_case_001", "hpc_execution", "COMPLETED")
        self.assertIsInstance(next_state, DownloadState)

    def test_hpc_execution_state_start_failure(self):
        """
        Test HpcExecutionState when failing to start the remote command.
        """
        state = HpcExecutionState()
        self.mock_context.remote_handler.execute_remote_command.return_value = (False, "", "SSH error")

        next_state = state.execute(self.mock_context)

        self.mock_context.db_handler.record_workflow_step.assert_any_call("test_case_001", "hpc_execution", "FAILED", "Failed to start HPC execution. Stderr: SSH error")
        self.assertIsNone(next_state)

    # --- DownloadState Tests ---

    def test_download_state_success(self):
        """
        Test DownloadState successful execution.
        """
        state = DownloadState()
        self.mock_context.remote_handler.download_files.return_value = TransferResult(success=True, message="OK", files_transferred=1)

        next_state = state.execute(self.mock_context)

        self.mock_context.remote_handler.download_files.assert_called_once()
        self.mock_context.db_handler.record_workflow_step.assert_any_call("test_case_001", "download", "COMPLETED")
        self.assertIsInstance(next_state, PostProcessingState)

    def test_download_state_failure(self):
        """
        Test DownloadState on handler failure.
        """
        state = DownloadState()
        self.mock_context.remote_handler.download_files.return_value = TransferResult(success=False, message="Download failed", files_transferred=0)

        next_state = state.execute(self.mock_context)

        self.mock_context.db_handler.record_workflow_step.assert_any_call("test_case_001", "download", "FAILED", "Download failed")
        self.assertIsNone(next_state)

    # --- PostProcessingState Tests ---

    def test_postprocessing_state_success(self):
        """
        Test PostProcessingState successful execution.
        """
        state = PostProcessingState()
        self.mock_context.local_handler.execute_raw_to_dicom.return_value = ExecutionResult(success=True, output="", error="", return_code=0)

        next_state = state.execute(self.mock_context)

        self.mock_context.local_handler.execute_raw_to_dicom.assert_called_once()
        self.mock_context.db_handler.record_workflow_step.assert_any_call("test_case_001", "postprocessing", "COMPLETED")
        self.mock_context.db_handler.update_case_status.assert_called_once_with("test_case_001", "COMPLETED", 100)
        self.assertIsNone(next_state) # Terminal state

    def test_postprocessing_state_failure(self):
        """
        Test PostProcessingState on handler failure.
        """
        state = PostProcessingState()
        self.mock_context.local_handler.execute_raw_to_dicom.return_value = ExecutionResult(success=False, output="", error="P3 failed", return_code=1)

        next_state = state.execute(self.mock_context)

        self.mock_context.db_handler.record_workflow_step.assert_any_call("test_case_001", "postprocessing", "FAILED", "P3 failed")
        self.mock_context.db_handler.update_case_status.assert_called_once_with("test_case_001", "FAILED")
        self.assertIsNone(next_state)

if __name__ == "__main__":
    unittest.main()