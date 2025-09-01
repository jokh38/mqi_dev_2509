"""
Sets up structured logging.
Integrates with legacy structured_logging.py for consistent log formatting.
"""
import logging
import json
from typing import Any, Dict, Optional
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from logging.handlers import RotatingFileHandler


@dataclass
class LogContext:
    """
    Context information for structured logging.
    Encapsulates common contextual data like case ID, operation type,
    and additional metadata for enhanced log observability.
    """
    case_id: Optional[str] = None
    operation: Optional[str] = None
    task_id: Optional[int] = None
    extra_data: Optional[Dict[str, Any]] = None

    def __post_init__(self):
        """Initialize extra_data as empty dict if not provided."""
        if self.extra_data is None:
            self.extra_data = {}

    def to_dict(self) -> Dict[str, Any]:
        """Convert context to dictionary for structured logging."""
        result = {}
        if self.case_id is not None:
            result["case_id"] = self.case_id
        if self.operation is not None:
            result["operation"] = self.operation
        if self.task_id is not None:
            result["task_id"] = self.task_id
        if self.extra_data:
            result.update(self.extra_data)
        return result


class StructuredLogger:
    """
    Enhanced logger that provides structured logging with context.
    """
    def __init__(self, name: str, default_context: Optional[Dict[str, Any]] = None):
        self.logger = logging.getLogger(name)
        self.default_context = default_context or {}

    def _build_context(self, context: Optional[LogContext] = None) -> Dict[str, Any]:
        full_context = self.default_context.copy()
        if context:
            full_context.update(context.to_dict())
        return full_context

    def _log_with_context(self, level: int, message: str, context: Optional[LogContext] = None, **kwargs):
        full_context = self._build_context(context)
        structured_message = format_structured_message(message, full_context)
        self.logger.log(level, structured_message, **kwargs)

    def debug(self, message: str, context: Optional[LogContext] = None, **kwargs):
        self._log_with_context(logging.DEBUG, message, context, **kwargs)

    def info(self, message: str, context: Optional[LogContext] = None, **kwargs):
        self._log_with_context(logging.INFO, message, context, **kwargs)

    def warning(self, message: str, context: Optional[LogContext] = None, **kwargs):
        self._log_with_context(logging.WARNING, message, context, **kwargs)

    def error(self, message: str, context: Optional[LogContext] = None, **kwargs):
        self._log_with_context(logging.ERROR, message, context, **kwargs)

    def critical(self, message: str, context: Optional[LogContext] = None, **kwargs):
        self._log_with_context(logging.CRITICAL, message, context, **kwargs)


def format_structured_message(message: str, context: Dict[str, Any]) -> str:
    if not context:
        return message
    context_parts = [f"{key}={value}" for key, value in context.items()]
    context_str = " ".join(context_parts)
    return f"{message} | {context_str}"


def get_structured_logger(name: str, default_context: Optional[Dict[str, Any]] = None) -> StructuredLogger:
    return StructuredLogger(name, default_context)


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        log_data = {
            "timestamp": datetime.fromtimestamp(record.created, timezone.utc).isoformat(),
            "level": record.levelname,
            "name": record.name,
            "message": record.getMessage(),
        }
        if "|" in record.message:
            message, context_str = record.message.split("|", 1)
            log_data["message"] = message.strip()
            try:
                context_data = dict(item.split("=") for item in context_str.strip().split(" "))
                for key, value in context_data.items():
                    try:
                        log_data[key] = json.loads(value)
                    except (json.JSONDecodeError, TypeError):
                        log_data[key] = value
            except ValueError:
                log_data["context"] = context_str.strip()
        if record.exc_info:
            log_data["exception"] = self.formatException(record.exc_info)
        return json.dumps(log_data)


class LoggingHandler:
    """
    Handler for structured logging setup and management.
    """
    def __init__(self, log_file_path: str = "mqi_communicator.log", log_level=logging.INFO, console_level=logging.DEBUG):
        self.log_file_path = Path(log_file_path)
        self.log_file_path.parent.mkdir(parents=True, exist_ok=True)

        root_logger = logging.getLogger()
        root_logger.setLevel(logging.DEBUG) # Set root logger to lowest level

        # File Handler with JSON Formatter
        file_handler = RotatingFileHandler(self.log_file_path, maxBytes=10*1024*1024, backupCount=5)
        file_handler.setLevel(log_level)
        file_handler.setFormatter(JsonFormatter())
        root_logger.addHandler(file_handler)

        # Console Handler with simple formatter
        console_handler = logging.StreamHandler()
        console_handler.setLevel(console_level)
        console_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        console_handler.setFormatter(console_formatter)
        root_logger.addHandler(console_handler)

    def get_logger(self, name: str, default_context: Optional[Dict[str, Any]] = None) -> StructuredLogger:
        return get_structured_logger(name, default_context)

    def get_case_logger(self, case_id: str) -> StructuredLogger:
        return self.get_logger("case_processor", {"case_id": case_id})

    def get_workflow_logger(self, case_id: str, operation: str) -> StructuredLogger:
        return self.get_logger("workflow", {"case_id": case_id, "operation": operation})

    def get_master_logger(self) -> StructuredLogger:
        return self.get_logger("master")

    def get_worker_logger(self, worker_id: int) -> StructuredLogger:
        return self.get_logger("worker", {"worker_id": worker_id})

    @staticmethod
    def shutdown():
        logging.shutdown()


def setup_logging(config: Dict[str, Any]) -> logging.Logger:
    # This function is for legacy compatibility.
    # In the new architecture, we would instantiate LoggingHandler directly.
    log_file = config.get("logging", {}).get("file", "mqi_communicator.log")
    log_level = config.get("logging", {}).get("level", "INFO").upper()
    LoggingHandler(log_file_path=log_file, log_level=getattr(logging, log_level))
    return logging.getLogger("mqi_communicator")