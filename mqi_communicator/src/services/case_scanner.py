import logging
import os
import time
from typing import Any, Dict

from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer

from src.common.db_manager import DatabaseManager
from src.common.structured_logging import get_structured_logger, LogContext


class _NewCaseHandler(FileSystemEventHandler):
    """Internal handler to process filesystem events."""

    def __init__(self, scanner_instance: "CaseScanner"):
        self.scanner = scanner_instance
        self.logger = get_structured_logger(self.__class__.__name__, {"component": "case_scanner_handler"})

    def on_created(self, event):
        """Called when a directory is created."""
        if event.is_directory:
            context = LogContext(
                operation="directory_creation_detected",
                extra_data={"case_path": event.src_path, "quiescence_period": self.scanner.quiescence_period}
            )
            self.logger.info(
                f"New directory detected: {event.src_path}. Waiting for quiescence...",
                context
            )
            # Wait for a short period to ensure file copy is complete
            time.sleep(self.scanner.quiescence_period)
            self.scanner._add_case_if_not_exists(event.src_path)


class CaseScanner:
    """Monitors a directory for new cases and adds them to the database."""

    def __init__(
        self, watch_path: str, db_manager: DatabaseManager, config: Dict[str, Any]
    ):
        self.watch_path = watch_path
        self.db_manager = db_manager
        self.config = config
        self.observer = Observer()
        self.logger = get_structured_logger(self.__class__.__name__, {"component": "case_scanner"})
        scanner_config = self.config.get("scanner", {})
        self.quiescence_period = scanner_config.get("quiescence_period_seconds", 5)

    def _add_case_if_not_exists(self, case_path: str):
        """
        Checks if a case exists in the DB and adds it if not.
        This centralizes the logic for both initial scan and the watchdog handler.
        """
        try:
            if not self.db_manager.get_case_by_path(case_path):
                self.db_manager.add_case(case_path)
                context = LogContext(
                    operation="case_registration",
                    extra_data={"case_path": case_path}
                )
                self.logger.info(f"Registered new case: {case_path}", context)
        except Exception as e:
            context = LogContext(
                operation="case_registration_failed",
                extra_data={"case_path": case_path}
            )
            self.logger.error_with_exception(
                f"Error processing case path '{case_path}'", e, context
            )

    def perform_initial_scan(self):
        """
        Scans the watch path for pre-existing directories upon startup
        and registers them if they are not already in the database.
        """
        context = LogContext(
            operation="initial_scan_start",
            extra_data={"watch_path": self.watch_path}
        )
        self.logger.info(
            f"Performing initial scan of '{self.watch_path}' for pre-existing cases...",
            context
        )
        try:
            for item_name in os.listdir(self.watch_path):
                item_path = os.path.join(self.watch_path, item_name)
                if os.path.isdir(item_path):
                    self._add_case_if_not_exists(item_path)
        except Exception as e:
            context = LogContext(
                operation="initial_scan_failed",
                extra_data={"watch_path": self.watch_path}
            )
            self.logger.error_with_exception(
                f"Error during initial scan of watch path", e, context
            )

    def start(self):
        """Starts the filesystem observer to watch for new directories."""
        event_handler = _NewCaseHandler(self)
        self.observer.schedule(event_handler, self.watch_path, recursive=False)
        self.observer.start()
        context = LogContext(
            operation="scanner_start",
            extra_data={"watch_path": self.watch_path}
        )
        self.logger.info(f"CaseScanner started, watching '{self.watch_path}'.", context)

    def stop(self):
        """Stops the filesystem observer."""
        if self.observer.is_alive():
            self.observer.stop()
            self.observer.join()
            context = LogContext(
                operation="scanner_stop",
                extra_data={"watch_path": self.watch_path}
            )
            self.logger.info("CaseScanner stopped.", context)