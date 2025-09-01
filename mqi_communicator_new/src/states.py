"""
Contains all State classes defining each workflow step.
Implements the State design pattern for workflow management.
"""
from abc import ABC, abstractmethod
from typing import Optional


class BaseState(ABC):
    """
    Abstract base class for all workflow states.
    
    Each concrete state implements the execute method to perform
    its specific task and return the next state in the workflow.
    """
    
    @abstractmethod
    def execute(self, context) -> Optional['BaseState']:
        """
        Execute the state's specific task.
        
        Args:
            context: The workflow context (WorkflowManager)
            
        Returns:
            The next state in the workflow, or None if this is a terminal state
        """
        pass


class PreProcessingState(BaseState):
    """
    State for handling local preprocessing using mqi_interpreter (P2).
    """
    
    def execute(self, context) -> Optional[BaseState]:
        """
        Execute local preprocessing step.
        
        Returns:
            FileUploadState on success, None on failure
        """
        # TODO: Implementation pattern:
        # try:
        #     context.logger.info("Starting preprocessing", LogContext(case_id=context.case_id, operation="preprocess"))
        #     context.db_handler.record_workflow_step(context.case_id, "preprocessing", "started")
        #     
        #     result = context.local_handler.execute_mqi_interpreter(context.case_id)
        #     if result.success:
        #         context.logger.info("Preprocessing completed successfully", LogContext(case_id=context.case_id, operation="preprocess"))
        #         context.db_handler.record_workflow_step(context.case_id, "preprocessing", "completed")
        #         return FileUploadState()
        #     else:
        #         context.logger.error(f"Preprocessing failed: {result.error}", LogContext(case_id=context.case_id, operation="preprocess"))
        #         context.db_handler.record_workflow_step(context.case_id, "preprocessing", "failed", result.error)
        #         return None
        # except Exception as e:
        #     context.logger.error_with_exception("Preprocessing exception", e, LogContext(case_id=context.case_id, operation="preprocess"))
        #     context.db_handler.record_workflow_step(context.case_id, "preprocessing", "error", str(e))
        #     return None
        pass  # Implementation will be added later


class FileUploadState(BaseState):
    """
    State for uploading files to HPC via SFTP.
    """
    
    def execute(self, context) -> Optional[BaseState]:
        """
        Upload necessary files to HPC.
        
        Returns:
            HpcExecutionState on success, None on failure
        """
        # TODO: Implementation pattern:
        # try:
        #     context.logger.info("Starting file upload", LogContext(case_id=context.case_id, operation="upload"))
        #     context.db_handler.record_workflow_step(context.case_id, "file_upload", "started")
        #     
        #     # Get paths from config
        #     local_dir = context.config.resolve_case_path(context.config.paths.local.processing_directory, context.case_id)
        #     remote_dir = context.config.resolve_case_path(context.config.paths.hpc.output_csv_dir, context.case_id)
        #     file_patterns = ["*.csv", "moqui_tps.in"]
        #     
        #     result = context.remote_handler.upload_files(local_dir, remote_dir, file_patterns)
        #     if result.success:
        #         context.logger.info(f"Upload completed: {result.files_transferred} files", LogContext(case_id=context.case_id, operation="upload"))
        #         context.db_handler.record_workflow_step(context.case_id, "file_upload", "completed")
        #         return HpcExecutionState()
        #     else:
        #         context.logger.error(f"Upload failed: {result.message}", LogContext(case_id=context.case_id, operation="upload"))
        #         context.db_handler.record_workflow_step(context.case_id, "file_upload", "failed", result.message)
        #         return None
        # except Exception as e:
        #     context.logger.error_with_exception("Upload exception", e, LogContext(case_id=context.case_id, operation="upload"))
        #     return None
        pass  # Implementation will be added later


class HpcExecutionState(BaseState):
    """
    State for executing MOQUI simulation on HPC via SSH.
    """
    
    def execute(self, context) -> Optional[BaseState]:
        """
        Execute MOQUI simulation on HPC.
        
        Returns:
            DownloadState on success, None on failure
        """
        # TODO: Implementation pattern:
        # try:
        #     context.logger.info("Starting HPC execution", LogContext(case_id=context.case_id, operation="hpc_execute"))
        #     context.db_handler.record_workflow_step(context.case_id, "hpc_execution", "started")
        #     
        #     # Build MOQUI execution command
        #     remote_dir = context.config.resolve_case_path(context.config.paths.hpc.output_csv_dir, context.case_id)
        #     command = f"cd {remote_dir} && moqui moqui_tps.in"
        #     
        #     success = context.remote_handler.execute_remote_command(command)
        #     if success:
        #         # Wait and poll for completion
        #         while True:
        #             status = context.remote_handler.check_job_status(context.case_id)
        #             if status == "completed":
        #                 context.logger.info("HPC execution completed", LogContext(case_id=context.case_id, operation="hpc_execute"))
        #                 context.db_handler.record_workflow_step(context.case_id, "hpc_execution", "completed")
        #                 return DownloadState()
        #             elif status == "failed":
        #                 context.logger.error("HPC execution failed", LogContext(case_id=context.case_id, operation="hpc_execute"))
        #                 context.db_handler.record_workflow_step(context.case_id, "hpc_execution", "failed")
        #                 return None
        #             time.sleep(context.config.application.polling_interval_seconds)
        #     else:
        #         context.logger.error("Failed to start HPC execution", LogContext(case_id=context.case_id, operation="hpc_execute"))
        #         return None
        # except Exception as e:
        #     context.logger.error_with_exception("HPC execution exception", e, LogContext(case_id=context.case_id, operation="hpc_execute"))
        #     return None
        pass  # Implementation will be added later


class DownloadState(BaseState):
    """
    State for downloading result files from HPC via SFTP.
    """
    
    def execute(self, context) -> Optional[BaseState]:
        """
        Download result files from HPC.
        
        Returns:
            PostProcessingState on success, None on failure
        """
        # TODO: Implementation pattern similar to FileUploadState:
        # - Log start of download operation
        # - Get remote and local paths from config
        # - Call remote_handler.download_files() with patterns ["*.raw"]
        # - Log results and update database
        # - Return PostProcessingState() on success, None on failure
        pass  # Implementation will be added later


class PostProcessingState(BaseState):
    """
    State for handling local postprocessing using RawToDCM (P3).
    """
    
    def execute(self, context) -> Optional[BaseState]:
        """
        Execute local postprocessing step.
        
        Returns:
            None (terminal state)
        """
        # TODO: Implementation pattern similar to PreProcessingState:
        # - Call local_handler.execute_raw_to_dicom()
        # - Log and record results
        # - Update case status to "completed" on success
        # - Return None (terminal state)
        pass  # Implementation will be added later


# TODO: Add error handling states:
# class ErrorState(BaseState):
#     """State for handling errors and cleanup"""
#     
# class RetryState(BaseState):
#     """State for implementing retry logic with exponential backoff"""