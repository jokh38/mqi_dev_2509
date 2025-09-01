"""
State Pattern-based workflow context manager.
Manages the execution flow of a case through different states.
"""
from typing import Optional
from pathlib import Path
from .states import BaseState, PreProcessingState
from .config import Config
from .database_handler import DatabaseHandler
from .local_handler import LocalHandler
from .remote_handler import RemoteHandler
from .logging_handler import StructuredLogger, LogContext


class WorkflowManager:
    """
    Context manager for the case workflow using the State pattern.
    """
    def __init__(self, case_id: str, case_path: Path, config: Config, db_handler: DatabaseHandler,
                 local_handler: LocalHandler, remote_handler: RemoteHandler, logger: StructuredLogger):
        self.case_id = case_id
        self.case_path = case_path
        self.config = config
        self.db_handler = db_handler
        self.local_handler = local_handler
        self.remote_handler = remote_handler
        self.logger = logger
        self.current_state: Optional[BaseState] = PreProcessingState()
        self.is_running = False

    def run_workflow(self):
        """
        Execute the main workflow loop.
        """
        self.is_running = True
        self.logger.info("Starting workflow", LogContext(case_id=self.case_id))
        self.db_handler.update_case_status(self.case_id, "PROCESSING", 0)

        try:
            while self.current_state is not None and self.is_running:
                self.current_state = self.current_state.execute(self)

            if self.is_running:
                self.logger.info("Workflow finished.", LogContext(case_id=self.case_id))
            else:
                self.logger.warning("Workflow was stopped.", LogContext(case_id=self.case_id))

        except Exception as e:
            self.logger.error(f"An unexpected error occurred in the workflow: {e}", LogContext(case_id=self.case_id))
            self.db_handler.update_case_status(self.case_id, "FAILED")
        finally:
            self.is_running = False
            self.remote_handler.close()

    def stop_workflow(self):
        """Gracefully stop the workflow."""
        self.is_running = False

    def get_current_status(self) -> str:
        """Get current workflow status."""
        if self.current_state is None:
            # Check DB for final status
            case_info = self.db_handler.get_case(self.case_id)
            return case_info.get("status", "UNKNOWN") if case_info else "UNKNOWN"
        return type(self.current_state).__name__