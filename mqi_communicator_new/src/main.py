"""
Master Process: Watches for cases, manages the process pool.
This is the main entry point for the application.
"""
import time
import multiprocessing
from pathlib import Path
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from typing import NoReturn, Dict, Any

from config import ConfigManager
from database_handler import DatabaseHandler
from logging_handler import LoggingHandler, LogContext
from worker import worker_main
from display_handler import DisplayHandler


class CaseDetectionHandler(FileSystemEventHandler):
    def __init__(self, case_queue: multiprocessing.Queue, db_handler: DatabaseHandler, logger, display: DisplayHandler):
        self.case_queue = case_queue
        self.db_handler = db_handler
        self.logger = logger
        self.display = display

    def on_created(self, event):
        if event.is_directory:
            case_path = Path(event.src_path)
            case_id = case_path.name
            log_context = LogContext(case_id=case_id)
            self.logger.info(f"New case detected: {case_id}", log_context)
            self.display.add_log_entry(f"New case detected: {case_id}")
            try:
                self.db_handler.add_case(case_id, str(case_path))
                self.case_queue.put((case_id, str(case_path)))
            except Exception as e:
                self.logger.error(f"Error adding case {case_id} to database: {e}", log_context)
                self.display.add_log_entry(f"ERROR: Could not add case {case_id} to DB.")


def main() -> NoReturn:
    """
    Main entry point for the MQI Communicator application.
    """
    config_manager = ConfigManager("config/config.yaml")
    config = config_manager.get_config()

    logging_handler = LoggingHandler()
    logger = logging_handler.get_master_logger()
    display = DisplayHandler()

    db_handler = DatabaseHandler(config.paths.local.database_path)

    case_queue = multiprocessing.Queue()
    status_queue = multiprocessing.Queue()

    active_workers: Dict[str, Any] = {}

    display.start()

    event_handler = CaseDetectionHandler(case_queue, db_handler, logger, display)
    observer = Observer()
    observer.schedule(event_handler, config.paths.local.scan_directory, recursive=False)
    observer.start()
    display.add_log_entry(f"Monitoring directory: {config.paths.local.scan_directory}")

    try:
        with multiprocessing.Pool(processes=config.application.max_workers) as pool:
            while True:
                # Dispatch new cases from the queue
                while not case_queue.empty() and len(active_workers) < config.application.max_workers:
                    case_id, case_path_str = case_queue.get()
                    result = pool.apply_async(worker_main, args=(case_id, case_path_str, status_queue))
                    active_workers[case_id] = result
                    display.add_case(case_id)
                    display.add_log_entry(f"Dispatched case {case_id} to worker pool.")

                # Process status updates from workers
                while not status_queue.empty():
                    case_id, status, progress = status_queue.get()
                    display.update_case_progress(case_id, status, progress)
                    if progress == 100 or "Failed" in status:
                        display.remove_case(case_id, status)
                        if case_id in active_workers:
                            del active_workers[case_id]

                # Update system status display
                display.update_system_status(len(active_workers), case_queue.qsize())

                time.sleep(1) # More responsive display loop

    except KeyboardInterrupt:
        display.add_log_entry("Shutdown signal received. Waiting for workers...")
        # pool.close() and pool.join() are handled by with statement
    finally:
        observer.stop()
        observer.join()
        db_handler.close()
        display.stop()
        logging_handler.shutdown()


if __name__ == "__main__":
    main()