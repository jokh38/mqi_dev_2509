import unittest
from unittest.mock import patch, MagicMock
from pathlib import Path
import subprocess

# Adjust the import path based on the project structure
from mqi_communicator_new.src.local_handler import LocalHandler, ExecutionResult
from mqi_communicator_new.src.config import Config

class TestLocalHandler(unittest.TestCase):
    """
    Test cases for the LocalHandler class.
    """

    def setUp(self):
        """
        Set up a mock config and LocalHandler instance for each test.
        """
        # Create a mock config object using MagicMock
        self.mock_config = MagicMock(spec=Config)

        # Mock the nested structure
        self.mock_config.executables = MagicMock()
        self.mock_config.paths = MagicMock()
        self.mock_config.paths.local = MagicMock()

        self.mock_config.executables.python_interpreter = "mock_python"
        self.mock_config.executables.mqi_interpreter = "mock_mqi_interpreter.py"
        self.mock_config.executables.raw_to_dicom = "mock_raw_to_dicom.py"
        self.mock_config.paths.local.processing_directory = "cases/{case_id}/processing"
        self.mock_config.paths.local.raw_output_directory = "cases/{case_id}/raw"
        self.mock_config.paths.local.final_dicom_directory = "cases/{case_id}/dicom"

        self.handler = LocalHandler(self.mock_config)

    @patch('mqi_communicator_new.src.local_handler.subprocess.run')
    def test_execute_subprocess_success(self, mock_run):
        """
        Test the _execute_subprocess method for a successful command execution.
        """
        # Configure the mock to return a successful process result
        mock_run.return_value = MagicMock(returncode=0, stdout="Success", stderr="")

        result = self.handler._execute_subprocess(["echo", "hello"])

        self.assertTrue(result.success)
        self.assertEqual(result.return_code, 0)
        self.assertEqual(result.output, "Success")
        self.assertEqual(result.error, "")
        mock_run.assert_called_once_with(["echo", "hello"], capture_output=True, text=True, check=False, timeout=300)

    @patch('mqi_communicator_new.src.local_handler.subprocess.run')
    def test_execute_subprocess_failure(self, mock_run):
        """
        Test the _execute_subprocess method for a failed command execution.
        """
        # Configure the mock to return a failed process result
        mock_run.return_value = MagicMock(returncode=1, stdout="", stderr="Error")

        result = self.handler._execute_subprocess(["invalid_command"])

        self.assertFalse(result.success)
        self.assertEqual(result.return_code, 1)
        self.assertEqual(result.error, "Error")

    @patch('mqi_communicator_new.src.local_handler.subprocess.run', side_effect=subprocess.TimeoutExpired(cmd="cmd", timeout=1))
    def test_execute_subprocess_timeout(self, mock_run):
        """
        Test the _execute_subprocess method for a command timeout.
        """
        result = self.handler._execute_subprocess(["sleep", "5"])

        self.assertFalse(result.success)
        self.assertIn("timed out", result.error)

    @patch('mqi_communicator_new.src.local_handler.subprocess.run', side_effect=FileNotFoundError)
    def test_execute_subprocess_not_found(self, mock_run):
        """
        Test the _execute_subprocess method for a FileNotFoundError.
        """
        result = self.handler._execute_subprocess(["non_existent_command"])

        self.assertFalse(result.success)
        self.assertIn("Executable not found", result.error)

    @patch('mqi_communicator_new.src.local_handler.LocalHandler._execute_subprocess')
    @patch('mqi_communicator_new.src.local_handler.Path.mkdir')
    def test_execute_mqi_interpreter(self, mock_mkdir, mock_execute):
        """
        Test the execute_mqi_interpreter method.
        """
        mock_execute.return_value = ExecutionResult(True, "output", "", 0)
        case_id = "test_case_123"
        case_path = Path("/path/to/case")

        result = self.handler.execute_mqi_interpreter(case_id, case_path)

        self.assertTrue(result.success)
        mock_mkdir.assert_called_once_with(parents=True, exist_ok=True)

        expected_command = [
            "mock_python",
            "mock_mqi_interpreter.py",
            "--dicom_input_folder", str(case_path),
            "--output_folder", f"cases/{case_id}/processing"
        ]
        mock_execute.assert_called_once_with(expected_command)

    @patch('mqi_communicator_new.src.local_handler.LocalHandler._execute_subprocess')
    @patch('mqi_communicator_new.src.local_handler.Path.mkdir')
    @patch('mqi_communicator_new.src.local_handler.Path.exists', return_value=True)
    @patch('mqi_communicator_new.src.local_handler.any', return_value=True)
    def test_execute_raw_to_dicom_success(self, mock_any, mock_exists, mock_mkdir, mock_execute):
        """
        Test the execute_raw_to_dicom method for a successful execution.
        """
        mock_execute.return_value = ExecutionResult(True, "output", "", 0)
        case_id = "test_case_456"

        result = self.handler.execute_raw_to_dicom(case_id)

        self.assertTrue(result.success)
        mock_mkdir.assert_called_once_with(parents=True, exist_ok=True)
        mock_exists.assert_called_once()
        mock_any.assert_called_once()

        raw_file_path = Path(f"cases/{case_id}/raw/dose.raw")
        expected_command = [
            "mock_python",
            "mock_raw_to_dicom.py",
            "--input_raw_file", str(raw_file_path),
            "--output_dicom_folder", f"cases/{case_id}/dicom"
        ]
        mock_execute.assert_called_once_with(expected_command)

    @patch('mqi_communicator_new.src.local_handler.Path.exists', return_value=False)
    def test_execute_raw_to_dicom_raw_file_not_found(self, mock_exists):
        """
        Test execute_raw_to_dicom when the raw file is missing.
        """
        case_id = "test_case_789"
        result = self.handler.execute_raw_to_dicom(case_id)

        self.assertFalse(result.success)
        self.assertIn("Raw file not found", result.error)

    @patch('mqi_communicator_new.src.local_handler.LocalHandler._execute_subprocess')
    @patch('mqi_communicator_new.src.local_handler.Path.mkdir')
    @patch('mqi_communicator_new.src.local_handler.Path.exists', return_value=True)
    @patch('mqi_communicator_new.src.local_handler.any', return_value=False) # Simulate no DICOM files created
    def test_execute_raw_to_dicom_no_dicom_created(self, mock_any, mock_exists, mock_mkdir, mock_execute):
        """
        Test execute_raw_to__dicom when the script runs but creates no .dcm files.
        """
        mock_execute.return_value = ExecutionResult(True, "output", "", 0)
        case_id = "test_case_101"

        result = self.handler.execute_raw_to_dicom(case_id)

        self.assertFalse(result.success)
        self.assertIn("no DICOM files were created", result.error)

from mqi_communicator_new.src.remote_handler import RemoteHandler, TransferResult

class TestRemoteHandler(unittest.TestCase):
    """
    Test cases for the RemoteHandler class.
    """

    def setUp(self):
        """
        Set up a mock config and RemoteHandler instance for each test.
        """
        self.mock_config = MagicMock(spec=Config)
        self.mock_config.hpc_connection = MagicMock()
        self.mock_config.hpc_connection.host = "mock_host"
        self.mock_config.hpc_connection.port = 22
        self.mock_config.hpc_connection.user = "mock_user"
        self.mock_config.hpc_connection.ssh_key_path = "/mock/key"

        # Patch paramiko.SSHClient
        self.ssh_client_patcher = patch('mqi_communicator_new.src.remote_handler.paramiko.SSHClient')
        self.mock_ssh_client_class = self.ssh_client_patcher.start()
        self.mock_ssh_client = self.mock_ssh_client_class.return_value

        self.handler = RemoteHandler(self.mock_config)
        self.handler.ssh_client = self.mock_ssh_client # Inject mock client

    def tearDown(self):
        """
        Stop the patcher after each test.
        """
        self.ssh_client_patcher.stop()

    def test_establish_connection(self):
        """
        Test that a connection is established if none exists.
        """
        # Un-set the injected client to test the _establish_connection method's logic
        self.handler.ssh_client = None
        self.mock_ssh_client.get_transport.return_value.is_active.return_value = False

        self.handler._establish_connection()

        self.mock_ssh_client_class.return_value.connect.assert_called_once_with(
            hostname="mock_host",
            port=22,
            username="mock_user",
            key_filename="/mock/key",
            timeout=10
        )

    def test_upload_files(self):
        """
        Test the upload_files method.
        """
        mock_sftp = self.mock_ssh_client.open_sftp.return_value
        local_dir = Path("/local/test_dir")

        # Mock glob to return different files for each pattern
        def glob_side_effect(pattern):
            if pattern == "*.csv":
                return [local_dir / "file1.csv"]
            elif pattern == "*.in":
                return [local_dir / "file2.in"]
            return []

        # We patch Path.glob for the instance `local_dir`
        with patch.object(Path, 'glob', side_effect=glob_side_effect) as mock_glob:
            result = self.handler.upload_files(local_dir, "/remote/test_dir", ["*.csv", "*.in"])

            self.assertTrue(result.success)
            self.assertEqual(result.files_transferred, 2)
            mock_sftp.put.assert_any_call(str(local_dir / "file1.csv"), "/remote/test_dir/file1.csv")
            mock_sftp.put.assert_any_call(str(local_dir / "file2.in"), "/remote/test_dir/file2.in")
            self.assertEqual(mock_sftp.put.call_count, 2)
            mock_sftp.close.assert_called_once()

    def test_download_files(self):
        """
        Test the download_files method.
        """
        mock_sftp = self.mock_ssh_client.open_sftp.return_value
        mock_sftp.listdir.return_value = ["file1.raw", "other.txt", "file2.raw"]
        local_dir = Path("/local/download_dir")
        with patch.object(Path, 'mkdir') as mock_mkdir:
            result = self.handler.download_files("/remote/data", local_dir, ["*.raw"])

            self.assertTrue(result.success)
            self.assertEqual(result.files_transferred, 2)
            mock_mkdir.assert_called_once_with(parents=True, exist_ok=True)
            mock_sftp.get.assert_any_call("/remote/data/file1.raw", str(local_dir / "file1.raw"))
            mock_sftp.get.assert_any_call("/remote/data/file2.raw", str(local_dir / "file2.raw"))
            self.assertEqual(mock_sftp.get.call_count, 2)
            mock_sftp.close.assert_called_once()

    def test_execute_remote_command_success(self):
        """
        Test successful remote command execution.
        """
        mock_stdin, mock_stdout, mock_stderr = MagicMock(), MagicMock(), MagicMock()
        self.mock_ssh_client.exec_command.return_value = (mock_stdin, mock_stdout, mock_stderr)

        mock_stdout.channel.recv_exit_status.return_value = 0
        mock_stdout.read.return_value = b"output"
        mock_stderr.read.return_value = b""

        success, stdout, stderr = self.handler.execute_remote_command("ls -l")

        self.assertTrue(success)
        self.assertEqual(stdout, "output")
        self.assertEqual(stderr, "")
        self.mock_ssh_client.exec_command.assert_called_once_with("ls -l", timeout=600)

    def test_execute_remote_command_failure(self):
        """
        Test failed remote command execution.
        """
        mock_stdin, mock_stdout, mock_stderr = MagicMock(), MagicMock(), MagicMock()
        self.mock_ssh_client.exec_command.return_value = (mock_stdin, mock_stdout, mock_stderr)

        mock_stdout.channel.recv_exit_status.return_value = 1
        mock_stdout.read.return_value = b""
        mock_stderr.read.return_value = b"error"

        success, stdout, stderr = self.handler.execute_remote_command("bad_command")

        self.assertFalse(success)
        self.assertEqual(stdout, "")
        self.assertEqual(stderr, "error")

    def test_check_job_completion_true(self):
        """
        Test check_job_completion when the marker file exists.
        """
        mock_sftp = self.mock_ssh_client.open_sftp.return_value
        mock_sftp.stat.return_value = True # Just needs to not raise an error

        result = self.handler.check_job_completion("/remote/dir", "done.marker")

        self.assertTrue(result)
        mock_sftp.stat.assert_called_once_with("/remote/dir/done.marker")
        mock_sftp.close.assert_called_once()

    def test_check_job_completion_false(self):
        """
        Test check_job_completion when the marker file does not exist.
        """
        mock_sftp = self.mock_ssh_client.open_sftp.return_value
        mock_sftp.stat.side_effect = FileNotFoundError

        result = self.handler.check_job_completion("/remote/dir", "done.marker")

        self.assertFalse(result)

    def test_close_connection(self):
        """
        Test that the close method closes the SSH client.
        """
        self.handler.close()
        self.mock_ssh_client.close.assert_called_once()

if __name__ == "__main__":
    unittest.main()