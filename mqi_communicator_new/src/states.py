"""
Contains all State classes defining each workflow step.
Implements the State design pattern for workflow management.
"""
import time
from abc import ABC, abstractmethod
from typing import Optional
from pathlib import Path
from .logging_handler import LogContext

class BaseState(ABC):
    """
    Abstract base class for all workflow states.
    """
    @abstractmethod
    def execute(self, context) -> Optional['BaseState']:
        """
        Execute the state's specific task.
        """
        pass


class PreProcessingState(BaseState):
    """
    State for handling local preprocessing using mqi_interpreter (P2).
    """
    def execute(self, context) -> Optional[BaseState]:
        log_context = LogContext(case_id=context.case_id, operation="preprocess")
        context.logger.info("Starting preprocessing", log_context)
        context.db_handler.record_workflow_step(context.case_id, "preprocessing", "STARTED")
        context.send_status_update("Preprocessing", 5)

        try:
            result = context.local_handler.execute_mqi_interpreter(context.case_id, context.case_path)
            if result.success:
                context.logger.info("Preprocessing completed successfully", log_context)
                context.db_handler.record_workflow_step(context.case_id, "preprocessing", "COMPLETED")
                context.send_status_update("Preprocessing", 20)
                return FileUploadState()
            else:
                context.logger.error(f"Preprocessing failed: {result.error}", log_context)
                context.db_handler.record_workflow_step(context.case_id, "preprocessing", "FAILED", result.error)
                context.send_status_update("Failed: Preprocessing", 5)
                return None
        except Exception as e:
            context.logger.error(f"Preprocessing exception: {e}", log_context)
            context.db_handler.record_workflow_step(context.case_id, "preprocessing", "FAILED", str(e))
            context.send_status_update("Failed: Preprocessing", 5)
            return None


class FileUploadState(BaseState):
    """
    State for uploading files to HPC via SFTP.
    """
    def execute(self, context) -> Optional[BaseState]:
        log_context = LogContext(case_id=context.case_id, operation="upload")
        context.logger.info("Starting file upload", log_context)
        context.db_handler.record_workflow_step(context.case_id, "file_upload", "STARTED")
        context.send_status_update("Uploading", 25)

        try:
            local_dir = Path(context.config.paths.local.processing_directory.format(case_id=context.case_id))
            remote_dir = context.config.paths.hpc.output_csv_dir.format(case_id=context.case_id)
            file_patterns = ["*.csv", "moqui_tps.in"]

            result = context.remote_handler.upload_files(local_dir, remote_dir, file_patterns)
            if result.success:
                context.logger.info(f"Upload completed: {result.files_transferred} files", log_context)
                context.db_handler.record_workflow_step(context.case_id, "file_upload", "COMPLETED")
                context.send_status_update("Uploading", 40)
                return HpcExecutionState()
            else:
                context.logger.error(f"Upload failed: {result.message}", log_context)
                context.db_handler.record_workflow_step(context.case_id, "file_upload", "FAILED", result.message)
                context.send_status_update("Failed: Upload", 25)
                return None
        except Exception as e:
            context.logger.error(f"Upload exception: {e}", log_context)
            context.db_handler.record_workflow_step(context.case_id, "file_upload", "FAILED", str(e))
            context.send_status_update("Failed: Upload", 25)
            return None


class HpcExecutionState(BaseState):
    """
    State for executing MOQUI simulation on HPC via SSH.
    """
    def execute(self, context) -> Optional[BaseState]:
        log_context = LogContext(case_id=context.case_id, operation="hpc_execute")
        context.logger.info("Starting HPC execution", log_context)
        context.db_handler.record_workflow_step(context.case_id, "hpc_execution", "STARTED")
        context.send_status_update("HPC Execution", 45)

        try:
            remote_dir = context.config.paths.hpc.output_csv_dir.format(case_id=context.case_id)
            command = f"cd {remote_dir} && moqui moqui_tps.in && touch moqui_done.marker"

            success, stdout, stderr = context.remote_handler.execute_remote_command(command)
            if not success:
                error_message = f"Failed to start HPC execution. Stderr: {stderr}"
                context.logger.error(error_message, log_context)
                context.db_handler.record_workflow_step(context.case_id, "hpc_execution", "FAILED", error_message)
                context.send_status_update("Failed: HPC Start", 45)
                return None

            context.logger.info("Polling for HPC completion...", log_context)
            context.send_status_update("Polling HPC", 50)
            remote_dose_dir = context.config.paths.hpc.dose_raw_dir.format(case_id=context.case_id)
            while True:
                if context.remote_handler.check_job_completion(remote_dose_dir, "dose.raw"):
                    context.logger.info("HPC execution completed", log_context)
                    context.db_handler.record_workflow_step(context.case_id, "hpc_execution", "COMPLETED")
                    context.send_status_update("HPC Execution", 70)
                    return DownloadState()

                time.sleep(context.config.application.polling_interval_seconds)

        except Exception as e:
            context.logger.error(f"HPC execution exception: {e}", log_context)
            context.db_handler.record_workflow_step(context.case_id, "hpc_execution", "FAILED", str(e))
            context.send_status_update("Failed: HPC Execution", 50)
            return None


class DownloadState(BaseState):
    """
    State for downloading result files from HPC via SFTP.
    """
    def execute(self, context) -> Optional[BaseState]:
        log_context = LogContext(case_id=context.case_id, operation="download")
        context.logger.info("Starting file download", log_context)
        context.db_handler.record_workflow_step(context.case_id, "download", "STARTED")
        context.send_status_update("Downloading", 75)

        try:
            remote_dir = context.config.paths.hpc.dose_raw_dir.format(case_id=context.case_id)
            local_dir = Path(context.config.paths.local.raw_output_directory.format(case_id=context.case_id))
            file_patterns = ["*.raw"]

            result = context.remote_handler.download_files(remote_dir, local_dir, file_patterns)
            if result.success:
                context.logger.info(f"Download completed: {result.files_transferred} files", log_context)
                context.db_handler.record_workflow_step(context.case_id, "download", "COMPLETED")
                context.send_status_update("Downloading", 90)
                return PostProcessingState()
            else:
                context.logger.error(f"Download failed: {result.message}", log_context)
                context.db_handler.record_workflow_step(context.case_id, "download", "FAILED", result.message)
                context.send_status_update("Failed: Download", 75)
                return None
        except Exception as e:
            context.logger.error(f"Download exception: {e}", log_context)
            context.db_handler.record_workflow_step(context.case_id, "download", "FAILED", str(e))
            context.send_status_update("Failed: Download", 75)
            return None


class PostProcessingState(BaseState):
    """
    State for handling local postprocessing using RawToDCM (P3).
    """
    def execute(self, context) -> Optional[BaseState]:
        log_context = LogContext(case_id=context.case_id, operation="postprocess")
        context.logger.info("Starting postprocessing", log_context)
        context.db_handler.record_workflow_step(context.case_id, "postprocessing", "STARTED")
        context.send_status_update("Post-processing", 95)

        try:
            result = context.local_handler.execute_raw_to_dicom(context.case_id)
            if result.success:
                context.logger.info("Postprocessing completed successfully", log_context)
                context.db_handler.record_workflow_step(context.case_id, "postprocessing", "COMPLETED")
                context.db_handler.update_case_status(context.case_id, "COMPLETED", 100)
                context.send_status_update("Completed", 100)
                return None # Terminal state
            else:
                context.logger.error(f"Postprocessing failed: {result.error}", log_context)
                context.db_handler.record_workflow_step(context.case_id, "postprocessing", "FAILED", result.error)
                context.db_handler.update_case_status(context.case_id, "FAILED")
                context.send_status_update("Failed: Post-processing", 95)
                return None
        except Exception as e:
            context.logger.error(f"Postprocessing exception: {e}", log_context)
            context.db_handler.record_workflow_step(context.case_id, "postprocessing", "FAILED", str(e))
            context.db_handler.update_case_status(context.case_id, "FAILED")
            context.send_status_update("Failed: Post-processing", 95)
            return None