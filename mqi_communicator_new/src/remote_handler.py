"""
Handles HPC communication (SSH/SFTP).
Manages remote execution and file transfer operations.
"""
import fnmatch
import time
from typing import NamedTuple, List, Any
from pathlib import Path
import paramiko
from .config import Config


class TransferResult(NamedTuple):
    """
    Structured result of a file transfer operation.
    """
    success: bool
    message: str
    files_transferred: int


class RemoteHandler:
    """
    Handler for HPC communication via SSH/SFTP.
    """
    def __init__(self, config: Config, max_retries: int = 3, retry_delay: int = 5):
        self.hpc_config = config.hpc_connection
        self.ssh_client = None
        self.max_retries = max_retries
        self.retry_delay = retry_delay

    def _retry_on_failure(self, operation: callable) -> Any:
        """Retry an operation with a simple backoff mechanism."""
        for attempt in range(self.max_retries):
            try:
                return operation()
            except (paramiko.SSHException, IOError) as e:
                if attempt == self.max_retries - 1:
                    raise e
                time.sleep(self.retry_delay * (attempt + 1))

    def _establish_connection(self):
        """Establish SSH connection if not already active."""
        if self.ssh_client and self.ssh_client.get_transport() and self.ssh_client.get_transport().is_active():
            return

        def connect():
            client = paramiko.SSHClient()
            client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            client.connect(
                hostname=self.hpc_config.host,
                port=self.hpc_config.port,
                username=self.hpc_config.user,
                key_filename=str(Path(self.hpc_config.ssh_key_path).expanduser()),
                timeout=10
            )
            self.ssh_client = client
        
        self._retry_on_failure(connect)

    def _create_remote_directory(self, sftp: paramiko.SFTPClient, remote_path: str):
        """Create a remote directory recursively."""
        dirs = []
        path = remote_path
        while path != '/':
            try:
                sftp.stat(path)
                break
            except FileNotFoundError:
                dirs.append(Path(path).name)
                path = str(Path(path).parent)
        
        for d in reversed(dirs):
            path = str(Path(path) / d)
            sftp.mkdir(path)

    def upload_files(self, local_dir: Path, remote_dir: str, file_patterns: List[str]) -> TransferResult:
        self._establish_connection()
        sftp = self.ssh_client.open_sftp()
        try:
            self._create_remote_directory(sftp, remote_dir)
            files_to_upload = []
            for pattern in file_patterns:
                files_to_upload.extend(list(local_dir.glob(pattern)))
            
            for local_file in files_to_upload:
                remote_file = f"{remote_dir}/{local_file.name}"
                sftp.put(str(local_file), remote_file)
            
            return TransferResult(True, f"Uploaded {len(files_to_upload)} files.", len(files_to_upload))
        finally:
            sftp.close()

    def download_files(self, remote_dir: str, local_dir: Path, file_patterns: List[str]) -> TransferResult:
        self._establish_connection()
        sftp = self.ssh_client.open_sftp()
        try:
            local_dir.mkdir(parents=True, exist_ok=True)
            remote_files = sftp.listdir(remote_dir)
            files_to_download = []
            for pattern in file_patterns:
                files_to_download.extend(fnmatch.filter(remote_files, pattern))
            
            for remote_file_name in files_to_download:
                remote_file_path = f"{remote_dir}/{remote_file_name}"
                local_file_path = local_dir / remote_file_name
                sftp.get(remote_file_path, str(local_file_path))

            return TransferResult(True, f"Downloaded {len(files_to_download)} files.", len(files_to_download))
        finally:
            sftp.close()

    def execute_remote_command(self, command: str, timeout: int = 600) -> tuple[bool, str, str]:
        self._establish_connection()
        stdin, stdout, stderr = self.ssh_client.exec_command(command, timeout=timeout)
        exit_code = stdout.channel.recv_exit_status()
        return exit_code == 0, stdout.read().decode(), stderr.read().decode()

    def check_job_completion(self, remote_dir: str, completion_marker: str) -> bool:
        """Check for the existence of a completion marker file."""
        self._establish_connection()
        sftp = self.ssh_client.open_sftp()
        try:
            sftp.stat(f"{remote_dir}/{completion_marker}")
            return True
        except FileNotFoundError:
            return False
        finally:
            sftp.close()

    def close(self):
        if self.ssh_client:
            self.ssh_client.close()