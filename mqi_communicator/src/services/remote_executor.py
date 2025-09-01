import subprocess
import shlex
import json
import re
import time
from pathlib import Path
from typing import Dict, Any, Optional, List, Literal, Tuple
import logging

from src.common.structured_logging import get_structured_logger, LogContext
from src.common.error_categorization import BaseExecutionError
from src.common.error_categorization import categorize_error

logger = get_structured_logger(__name__)


class RemoteExecutionError(BaseExecutionError):
    """Custom exception for errors during remote execution."""
    
    def __init__(self, message: str, error_type: str = "general", stderr: Optional[str] = None):
        details = {"error_type": error_type}
        if stderr:
            details["stderr"] = stderr
        super().__init__(message, details)


class RemoteExecutor:
    """
    Handles all remote interactions including file transfers, command execution, and monitoring.
    
    This class is responsible for managing the complete remote workflow lifecycle
    including file uploads/downloads, pueue job submission, and status monitoring.
    """

    def __init__(self, config: Dict[str, Any]) -> None:
        """
        Initialize the RemoteExecutor with configuration.
        """
        self.config = config # Store the full config
        self.hpc_config = config.get("hpc", {})
        self.user = self.hpc_config.get("user")
        self.host = self.hpc_config.get("host")
        self.ssh_cmd = self.hpc_config.get("ssh_command", "ssh")
        self.scp_cmd = self.hpc_config.get("scp_command", "scp")
        self.pueue_cmd = self.hpc_config.get("pueue_command", "pueue")

    def execute(self, target: str, context: Dict[str, Any], display = None) -> Dict[str, Any]:
        """
        Execute a remote operation with monitoring.
        
        Args:
            target: The target operation to execute (e.g., 'run_moqui')
            context: Execution context containing case information and parameters
            display: Optional RichProgressDisplay instance for UI updates
            
        Returns:
            Dictionary containing execution results and metadata
            
        Raises:
            RemoteExecutionError: If execution fails
        """
        if target == "run_moqui":
            return self._run_moqui(context, display)
        else:
            raise RemoteExecutionError(f"Unknown remote execution target: {target}")

    def _run_moqui(self, context: Dict[str, Any], display = None) -> Dict[str, Any]:
        """
        Run the complete MOQUI workflow on the remote HPC.
        
        Args:
            context: Execution context with case information
            display: Optional progress display instance
            
        Returns:
            Dictionary with execution results
        """
        case_id = context.get("case_id")
        case_path = context["case_path"]
        run_id = context.get("run_id", "default")
        pueue_group = context.get("pueue_group", "default")
        
        case_name = Path(case_path).name
        remote_case_dir = f"{self.hpc_config['remote_base_dir']}/{case_name}/{run_id}"
        
        logger.info(
            "Starting remote MOQUI execution",
            context=LogContext(
                case_id=str(case_id) if case_id else None,
                operation="remote_moqui",
                extra_data={
                    "case_name": case_name,
                    "run_id": run_id,
                    "pueue_group": pueue_group
                }
            ).to_dict()
        )
        
        try:
            # Step 1: Create remote directories
            if display:
                display.update_status("Creating remote directories...")
            self._create_remote_directories(case_id, case_name, run_id)
            
            # Step 2: Generate and upload moqui_tps.in file
            if display:
                display.update_status("Generating moqui_tps.in file...")
            self._generate_and_upload_tps_file(case_id, case_path, run_id, display)
            
            # Step 3: Upload case files
            if display:
                display.update_status("Uploading case files...")
                display.update_progress(10)
            self._upload_files(case_id, case_path, remote_case_dir, display)
            
            # Step 4: Submit and monitor pueue job
            if display:
                display.update_status("Submitting remote job...")
                display.update_progress(30)
            task_id = self._submit_pueue_job(case_id, case_name, remote_case_dir, pueue_group, display)
            
            # Step 5: Monitor job execution
            if display:
                display.update_status("Monitoring remote execution...")
                display.update_progress(40)
            job_result = self._monitor_job_execution(task_id, case_id, display)
            
            if job_result["status"] != "success":
                raise RemoteExecutionError(
                    f"Remote MOQUI execution failed: {job_result.get('error', 'Unknown error')}",
                    error_type="execution_failure"
                )
            
            # Step 6: Download results
            if display:
                display.update_status("Downloading results...")
                display.update_progress(80)
            downloaded_files = self._download_results(case_id, case_path, remote_case_dir, display)
            
            if display:
                display.update_progress(100)
                display.complete_step()
            
            logger.info(
                "Remote MOQUI execution completed successfully",
                context=LogContext(
                    case_id=str(case_id) if case_id else None,
                    operation="remote_moqui",
                    extra_data={
                        "task_id": task_id,
                        "downloaded_files": len(downloaded_files),
                        "execution_time": job_result.get("execution_time_seconds")
                    }
                ).to_dict()
            )
            
            return {
                "success": True,
                "task_id": task_id,
                "downloaded_files": downloaded_files,
                "execution_time_seconds": job_result.get("execution_time_seconds"),
                "remote_case_dir": remote_case_dir
            }
            
        except Exception as e:
            error_category = categorize_error(e, "remote_moqui")
            
            if display:
                display.set_error(f"Remote execution failed: {str(e)}")
                
            logger.error_with_exception(
                "Remote MOQUI execution failed",
                e,
                context=LogContext(
                    case_id=str(case_id) if case_id else None,
                    operation="remote_moqui",
                    error_category=error_category,
                    extra_data={"case_path": case_path, "run_id": run_id}
                ).to_dict()
            )
            raise

    def _create_remote_directories(self, case_id: Optional[int], case_name: str, run_id: str) -> None:
        """
        Create necessary remote directories for the case and run.
        
        Args:
            case_id: Database ID of the case
            case_name: Name of the case directory
            run_id: Unique identifier for this run
        """
        try:
            directories_to_create = [
                f"{self.hpc_config['remote_base_dir']}/{case_name}/{run_id}",
                f"{self.hpc_config.get('moqui_interpreter_outputs_dir', '~/Outputs_csv')}/{case_name}",
                f"{self.hpc_config.get('moqui_outputs_dir', '~/Dose_raw')}/{case_name}"
            ]
            
            for directory in directories_to_create:
                mkdir_command = [
                    self.ssh_cmd,
                    f"{self.user}@{self.host}",
                    f"mkdir -p {shlex.quote(directory)}"
                ]
                
                result = subprocess.run(
                    mkdir_command,
                    check=True,
                    capture_output=True,
                    text=True,
                    timeout=30
                )
                
            logger.info(
                "Remote directories created successfully",
                context=LogContext(
                    case_id=str(case_id) if case_id else None,
                    operation="directory_creation",
                    extra_data={
                        "case_name": case_name,
                        "run_id": run_id,
                        "directories": directories_to_create
                    }
                ).to_dict()
            )
            
        except subprocess.CalledProcessError as e:
            error_msg = f"Failed to create remote directories for {case_name}/{run_id}"
            logger.error_with_exception(
                error_msg,
                e,
                context=LogContext(
                    case_id=str(case_id) if case_id else None,
                    operation="directory_creation",
                    extra_data={
                        "case_name": case_name,
                        "run_id": run_id,
                        "error_details": e.stderr
                    }
                ).to_dict()
            )
            raise RemoteExecutionError(error_msg, error_type="network", stderr=e.stderr) from e

    def _generate_and_upload_tps_file(
        self, 
        case_id: Optional[int], 
        case_path: str, 
        run_id: str,
        display = None
    ) -> None:
        """
        Generate moqui_tps.in file and upload it to the remote server.
        
        Args:
            case_id: Database ID of the case
            case_path: Local path to the case directory
            run_id: Unique identifier for this run
            display: Optional progress display instance
        """
        from src.services.tps_generator import create_ini_content, validate_ini_content
        from src.common.dicom_parser import find_rtplan_file, get_plan_info
        
        case_name = Path(case_path).name
        remote_case_dir = f"{self.hpc_config['remote_base_dir']}/{case_name}/{run_id}"
        
        try:
            # Extract DICOM information
            dicom_info = None
            try:
                rtplan_file = find_rtplan_file(case_path)
                dicom_info = get_plan_info(rtplan_file)
                if display:
                    display.update_subtask(f"Extracted DICOM info from {Path(rtplan_file).name}")
            except Exception as e:
                logger.warning_with_exception(
                    f"Failed to extract DICOM info for case {case_name}, using defaults",
                    e,
                    context=LogContext(
                        case_id=str(case_id) if case_id else None,
                        operation="dicom_extraction"
                    ).to_dict()
                )
                if display:
                    display.update_subtask("Using default DICOM parameters")

            # Prepare case data for TPS generation
            case_data = {
                'case_id': case_id,
                'case_path': case_path,
                'pueue_group': context.get('pueue_group', 'default')
            }
            
            # Load parameters from config
            base_params = self.config.get("moqui_tps_parameters", {})
            tps_generator_config = self.config.get("tps_generator", {})

            # Generate INI content
            ini_content = create_ini_content(
                case_data,
                base_params,
                dicom_info,
                self.hpc_config,
                tps_generator_config
            )

            # Validate INI content
            validation_rules = tps_generator_config.get("validation", {})
            required_params = validation_rules.get("required_params", [])
            
            if not validate_ini_content(ini_content, required_params):
                raise RemoteExecutionError(
                    f"Generated moqui_tps.in for case {case_data.get('case_id')} failed validation",
                    error_type="validation"
                )
            
            if display:
                display.update_subtask(f"Generated moqui_tps.in ({len(ini_content)} bytes)")
            
            # Upload INI file to remote server
            remote_ini_path = f"{remote_case_dir}/moqui_tps.in"
            ssh_command = [
                self.ssh_cmd,
                f"{self.user}@{self.host}",
                f"cat > {shlex.quote(remote_ini_path)}"
            ]
            
            result = subprocess.run(
                ssh_command,
                input=ini_content,
                check=True,
                capture_output=True,
                text=True,
                timeout=60
            )
            
            logger.info(
                "moqui_tps.in file generated and uploaded successfully",
                context=LogContext(
                    case_id=str(case_id) if case_id else None,
                    operation="tps_file_upload",
                    extra_data={
                        "case_name": case_name,
                        "run_id": run_id,
                        "content_size": len(ini_content),
                        "remote_path": remote_ini_path
                    }
                ).to_dict()
            )
            
            if display:
                display.update_subtask("moqui_tps.in uploaded successfully")
            
        except subprocess.CalledProcessError as e:
            error_msg = f"Failed to upload moqui_tps.in file for {case_name}/{run_id}"
            logger.error_with_exception(
                error_msg,
                e,
                context=LogContext(
                    case_id=str(case_id) if case_id else None,
                    operation="tps_file_upload",
                    extra_data={
                        "case_name": case_name,
                        "run_id": run_id,
                        "error_details": e.stderr
                    }
                ).to_dict()
            )
            raise RemoteExecutionError(error_msg, error_type="network", stderr=e.stderr) from e

    def _upload_files(
        self, 
        case_id: Optional[int], 
        local_source: str, 
        remote_destination: str,
        display = None
    ) -> None:
        """
        Upload files from local source to remote destination using scp.
        
        Args:
            case_id: Database ID of the case
            local_source: Local source path
            remote_destination: Remote destination path
            display: Optional progress display instance
        """
        try:
            # Normalize path for cross-platform compatibility
            normalized_source = local_source.replace('\\', '/')
            
            # Handle tilde expansion for remote path
            remote_dest_for_scp = remote_destination
            if remote_dest_for_scp.startswith('~/'):
                remote_dest_for_scp = remote_dest_for_scp[2:]
            
            scp_command = [
                self.scp_cmd,
                "-r",
                normalized_source,
                f"{self.user}@{self.host}:{remote_dest_for_scp}",
            ]
            
            if display:
                display.update_subtask(f"Uploading files to {Path(remote_destination).name}")
            
            result = subprocess.run(
                scp_command,
                check=True,
                capture_output=True,
                text=True,
                timeout=300  # 5 minute timeout for file transfers
            )
            
            logger.info(
                "Files uploaded successfully",
                context=LogContext(
                    case_id=str(case_id) if case_id else None,
                    operation="file_upload",
                    extra_data={
                        "local_source": local_source,
                        "remote_destination": remote_destination
                    }
                ).to_dict()
            )
            
            if display:
                display.update_subtask("File upload completed")
                display.update_progress(25)
            
        except subprocess.CalledProcessError as e:
            error_msg = f"Failed to upload files from {local_source} to {remote_destination}"
            logger.error_with_exception(
                error_msg,
                e,
                context=LogContext(
                    case_id=str(case_id) if case_id else None,
                    operation="file_upload",
                    extra_data={
                        "local_source": local_source,
                        "remote_destination": remote_destination,
                        "error_details": e.stderr
                    }
                ).to_dict()
            )
            raise RemoteExecutionError(error_msg, error_type="network", stderr=e.stderr) from e
        except subprocess.TimeoutExpired as e:
            error_msg = f"Timeout during file upload from {local_source}"
            logger.error_with_exception(
                error_msg,
                e,
                context=LogContext(
                    case_id=str(case_id) if case_id else None,
                    operation="file_upload",
                    extra_data={
                        "local_source": local_source,
                        "timeout_seconds": 300
                    }
                ).to_dict()
            )
            raise RemoteExecutionError(error_msg, error_type="timeout") from e

    def _submit_pueue_job(
        self,
        case_id: Optional[int],
        case_name: str,
        remote_case_dir: str,
        pueue_group: str,
        display = None
    ) -> int:
        """
        Submit a pueue job for the case and return the task ID.
        
        Args:
            case_id: Database ID of the case
            case_name: Name of the case
            remote_case_dir: Remote directory containing the case
            pueue_group: Pueue group for job submission
            display: Optional progress display instance
            
        Returns:
            Task ID of the submitted job
        """
        label = f"mqic_case_{case_id}_{int(time.time())}" if case_id else f"mqic_{case_name}_{int(time.time())}"
        
        # Use the first workflow step from legacy configuration
        # In a full implementation, this would be configurable
        command = f"cd {shlex.quote(remote_case_dir)} && ~/tps_env/.tps_env"
        
        ssh_command = [
            self.ssh_cmd,
            f"{self.user}@{self.host}",
            self.pueue_cmd,
            "add",
            "--label", label,
            "--group", pueue_group,
            "--", "sh", "-c", command
        ]
        
        try:
            if display:
                display.update_subtask(f"Submitting job to {pueue_group}")
            
            result = subprocess.run(
                ssh_command,
                check=True,
                capture_output=True,
                text=True,
                timeout=60
            )
            
            task_id = self._parse_pueue_add_output(result.stdout)
            if task_id is None:
                raise RemoteExecutionError(
                    f"Failed to parse task ID from pueue output: {result.stdout}",
                    error_type="parsing"
                )
            
            logger.info(
                "Pueue job submitted successfully",
                context=LogContext(
                    case_id=str(case_id) if case_id else None,
                    operation="job_submission",
                    extra_data={
                        "task_id": task_id,
                        "label": label,
                        "pueue_group": pueue_group
                    }
                ).to_dict()
            )
            
            if display:
                display.update_subtask(f"Job submitted (Task ID: {task_id})")
            
            return task_id
            
        except subprocess.CalledProcessError as e:
            error_msg = f"Failed to submit pueue job for {case_name}"
            logger.error_with_exception(
                error_msg,
                e,
                context=LogContext(
                    case_id=str(case_id) if case_id else None,
                    operation="job_submission",
                    extra_data={
                        "case_name": case_name,
                        "pueue_group": pueue_group,
                        "error_details": e.stderr
                    }
                ).to_dict()
            )
            raise RemoteExecutionError(error_msg, error_type="network", stderr=e.stderr) from e

    def _parse_pueue_add_output(self, output: str) -> Optional[int]:
        """Parse the output of `pueue add` to find the task ID."""
        match = re.search(r"\(id: (\d+)\)", output)
        return int(match.group(1)) if match else None

    def _monitor_job_execution(
        self,
        task_id: int,
        case_id: Optional[int],
        display = None,
        poll_interval: int = 30
    ) -> Dict[str, Any]:
        """
        Monitor the execution of a pueue job until completion.
        
        Args:
            task_id: Task ID to monitor
            case_id: Database ID of the case
            display: Optional progress display instance
            poll_interval: Seconds between status polls
            
        Returns:
            Dictionary containing job execution results
        """
        start_time = time.time()
        last_status = None
        progress = 40  # Start at 40% since job is submitted
        
        while True:
            try:
                status = self.get_workflow_status(task_id)
                
                if status != last_status:
                    logger.info(
                        f"Job status changed: {status}",
                        context=LogContext(
                            case_id=str(case_id) if case_id else None,
                            operation="job_monitoring",
                            extra_data={"task_id": task_id, "status": status}
                        ).to_dict()
                    )
                    last_status = status
                    
                    if display:
                        if status == "running":
                            display.update_subtask("Job is running on HPC...")
                            progress = min(70, progress + 10)
                        elif status == "queued":
                            display.update_subtask("Job is queued...")
                        display.update_progress(progress)
                
                if status == "success":
                    execution_time = time.time() - start_time
                    return {
                        "status": "success",
                        "execution_time_seconds": execution_time,
                        "task_id": task_id
                    }
                elif status == "failure":
                    execution_time = time.time() - start_time
                    return {
                        "status": "failure",
                        "error": f"Remote job {task_id} failed",
                        "execution_time_seconds": execution_time,
                        "task_id": task_id
                    }
                elif status == "not_found":
                    return {
                        "status": "failure",
                        "error": f"Task {task_id} not found in pueue",
                        "task_id": task_id
                    }
                elif status == "unreachable":
                    raise RemoteExecutionError(
                        f"HPC unreachable while monitoring task {task_id}",
                        error_type="network"
                    )
                
                # Continue monitoring
                time.sleep(poll_interval)
                
                # Increment progress slightly during long running jobs
                if status == "running":
                    progress = min(75, progress + 1)
                    if display:
                        display.update_progress(progress)
                
            except KeyboardInterrupt:
                logger.warning(
                    "Job monitoring interrupted by user",
                    context=LogContext(
                        case_id=str(case_id) if case_id else None,
                        operation="job_monitoring",
                        extra_data={"task_id": task_id}
                    ).to_dict()
                )
                return {
                    "status": "interrupted",
                    "error": "Monitoring interrupted by user",
                    "task_id": task_id
                }

    def _download_results(
        self,
        case_id: Optional[int],
        local_case_path: str,
        remote_case_dir: str,
        display = None
    ) -> List[str]:
        """
        Download result files from the remote case directory.
        
        Args:
            case_id: Database ID of the case
            local_case_path: Local case directory path
            remote_case_dir: Remote case directory path
            display: Optional progress display instance
            
        Returns:
            List of downloaded file paths
        """
        local_output_dir = Path(local_case_path) / "raw_output"
        local_output_dir.mkdir(parents=True, exist_ok=True)
        
        case_name = Path(local_case_path).name
        
        try:
            # Download dose files from remote dose directory
            remote_dose_path = f"{self.hpc_config.get('moqui_outputs_dir', '~/Dose_raw')}/{case_name}/*"
            
            if display:
                display.update_subtask("Downloading dose files...")
            
            scp_command = [
                self.scp_cmd,
                "-r",
                f"{self.user}@{self.host}:{remote_dose_path}",
                str(local_output_dir)
            ]
            
            result = subprocess.run(
                scp_command,
                check=True,
                capture_output=True,
                text=True,
                timeout=300
            )
            
            # List downloaded files
            downloaded_files = []
            for file_path in local_output_dir.rglob("*"):
                if file_path.is_file():
                    downloaded_files.append(str(file_path))
            
            logger.info(
                "Results downloaded successfully",
                context=LogContext(
                    case_id=str(case_id) if case_id else None,
                    operation="result_download",
                    extra_data={
                        "case_name": case_name,
                        "downloaded_files": len(downloaded_files),
                        "local_output_dir": str(local_output_dir)
                    }
                ).to_dict()
            )
            
            if display:
                display.update_subtask(f"Downloaded {len(downloaded_files)} files")
                display.update_progress(95)
            
            return downloaded_files
            
        except subprocess.CalledProcessError as e:
            error_msg = f"Failed to download results for case {case_name}"
            logger.error_with_exception(
                error_msg,
                e,
                context=LogContext(
                    case_id=str(case_id) if case_id else None,
                    operation="result_download",
                    extra_data={
                        "case_name": case_name,
                        "remote_dose_path": remote_dose_path,
                        "error_details": e.stderr
                    }
                ).to_dict()
            )
            raise RemoteExecutionError(error_msg, error_type="network", stderr=e.stderr) from e

    def get_workflow_status(self, task_id: int) -> Literal["success", "failure", "running", "not_found", "unreachable"]:
        """
        Check the status of a specific workflow task in Pueue.
        
        Args:
            task_id: The ID of the task to check
            
        Returns:
            String representing the task status
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

            if str(task_id) not in tasks:
                return "not_found"

            task_info = tasks[str(task_id)]
            status = task_info.get("status")

            if status == "Done":
                return "success" if task_info.get("result") == "success" else "failure"
            elif status in ["Failed", "Killing"]:
                return "failure"
            else:  # 'Running', 'Queued', 'Paused', etc.
                return "running"

        except (
            subprocess.TimeoutExpired,
            subprocess.CalledProcessError,
            json.JSONDecodeError,
            KeyError,
        ) as e:
            logger.warning_with_exception(
                "Failed to get task status from HPC",
                e,
                context=LogContext(
                    operation="task_status_check",
                    extra_data={"task_id": task_id}
                ).to_dict()
            )
            return "unreachable"