"""
Master Process: Watches for cases, manages the process pool.
This is the main entry point for the application.
"""
import time
import multiprocessing
from pathlib import Path
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from typing import NoReturn

from config import ConfigManager
from database_handler import DatabaseHandler
from logging_handler import LoggingHandler, LogContext
from worker import worker_main


class CaseDetectionHandler(FileSystemEventHandler):
    def __init__(self, case_queue: multiprocessing.Queue, db_handler: DatabaseHandler, logger):
        self.case_queue = case_queue
        self.db_handler = db_handler
        self.logger = logger

    def on_created(self, event):
        if event.is_directory:
            case_path = Path(event.src_path)
            case_id = case_path.name
            self.logger.info(f"New case detected: {case_id}", LogContext(case_id=case_id))
            try:
                self.db_handler.add_case(case_id, str(case_path))
                self.case_queue.put((case_id, str(case_path)))
            except Exception as e:
                self.logger.error(f"Error adding case {case_id} to database: {e}", LogContext(case_id=case_id))


def main() -> NoReturn:
    """
    Main entry point for the MQI Communicator application.
    """
    config_manager = ConfigManager("config/config.yaml")
    config = config_manager.get_config()

    logging_handler = LoggingHandler()
    logger = logging_handler.get_master_logger()

    db_path = str(Path(config.paths.local.scan_directory).parent / "database/mqi_communicator.db")
    db_handler = DatabaseHandler(db_path)

    case_queue = multiprocessing.Queue()
    status_queue = multiprocessing.Queue()

    # Start directory monitoring
    event_handler = CaseDetectionHandler(case_queue, db_handler, logger)
    observer = Observer()
    observer.schedule(event_handler, config.paths.local.scan_directory, recursive=False)
    observer.start()
    logger.info(f"Started monitoring directory: {config.paths.local.scan_directory}")

    try:
        with multiprocessing.Pool(processes=config.application.max_workers) as pool:
            while True:
                # Check for new cases and dispatch them
                if not case_queue.empty():
                    case_id, case_path_str = case_queue.get()
                    pool.apply_async(worker_main, args=(case_id, case_path_str, status_queue))
                    logger.info(f"Dispatched case {case_id} to a worker.", LogContext(case_id=case_id))

                # Process status updates from workers (basic implementation)
                while not status_queue.empty():
                    case_id, status, message = status_queue.get()
                    logger.info(f"STATUS from {case_id}: {status} - {message}", LogContext(case_id=case_id))

                time.sleep(config.application.scan_interval_seconds)

    except KeyboardInterrupt:
        logger.info("Shutting down gracefully...")
    finally:
        observer.stop()
        observer.join()
        db_handler.close()
        logging_handler.shutdown()


if __name__ == "__main__":
    main()