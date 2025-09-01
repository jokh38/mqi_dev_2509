"""
Structured logging implementation for enhanced context and observability.
Provides consistent log formatting with contextual information.
"""

import logging
import json
from typing import Any, Dict, Optional
from dataclasses import dataclass
from datetime import datetime
from .error_categorization import categorize_error, ErrorCategory


@dataclass
class LogContext:
    """
    Context information for structured logging.

    Encapsulates common contextual data like case ID, operation type,
    and additional metadata for enhanced log observability.
    """

    case_id: Optional[str] = None
    operation: Optional[str] = None
    gpu_group: Optional[str] = None
    task_id: Optional[int] = None
    error_category: Optional[ErrorCategory] = None
    is_retryable: Optional[bool] = None
    extra_data: Optional[Dict[str, Any]] = None

    def __post_init__(self):
        """Initialize extra_data as empty dict if not provided."""
        if self.extra_data is None:
            self.extra_data = {}

    def to_dict(self) -> Dict[str, Any]:
        """Convert context to dictionary for structured logging."""
        result = {}

        # Add non-None fields
        if self.case_id is not None:
            result["case_id"] = self.case_id
        if self.operation is not None:
            result["operation"] = self.operation
        if self.gpu_group is not None:
            result["gpu_group"] = self.gpu_group
        if self.task_id is not None:
            result["task_id"] = self.task_id
        if self.error_category is not None:
            result["error_category"] = self.error_category.value
        if self.is_retryable is not None:
            result["is_retryable"] = self.is_retryable

        # Merge extra_data
        if self.extra_data:
            result.update(self.extra_data)

        return result


class StructuredLogger:
    """
    Enhanced logger that provides structured logging with context.

    Wraps standard Python logging to include consistent contextual information
    and structured formatting for better observability.
    """

    def __init__(self, name: str, default_context: Optional[Dict[str, Any]] = None):
        """
        Initialize structured logger.

        Args:
            name: Logger name
            default_context: Default context included in all log messages
        """
        self.logger = logging.getLogger(name)
        self.default_context = default_context or {}

    def _build_context(self, context: Optional[LogContext] = None) -> Dict[str, Any]:
        """Build complete context by merging default and specific context."""
        full_context = self.default_context.copy()

        if context:
            full_context.update(context.to_dict())

        return full_context

    def _log_with_context(
        self, level: int, message: str, context: Optional[LogContext] = None, **kwargs
    ):
        """Internal method to log with structured context."""
        full_context = self._build_context(context)
        structured_message = format_structured_message(message, full_context)
        self.logger.log(level, structured_message, **kwargs)

    def debug(self, message: str, context: Optional[LogContext] = None, **kwargs):
        """Log debug message with context."""
        self._log_with_context(logging.DEBUG, message, context, **kwargs)

    def info(self, message: str, context: Optional[LogContext] = None, **kwargs):
        """Log info message with context."""
        self._log_with_context(logging.INFO, message, context, **kwargs)

    def warning(self, message: str, context: Optional[LogContext] = None, **kwargs):
        """Log warning message with context."""
        self._log_with_context(logging.WARNING, message, context, **kwargs)

    def error(self, message: str, context: Optional[LogContext] = None, **kwargs):
        """Log error message with context."""
        self._log_with_context(logging.ERROR, message, context, **kwargs)

    def critical(self, message: str, context: Optional[LogContext] = None, **kwargs):
        """Log critical message with context."""
        self._log_with_context(logging.CRITICAL, message, context, **kwargs)

    def error_with_exception(self, message: str, exception: Exception, context: Optional[LogContext] = None, **kwargs):
        """
        Log error message with automatic error categorization.
        
        Args:
            message: The main error message
            exception: The exception to categorize
            context: Optional log context
            **kwargs: Additional logging arguments
        """
        category, is_retryable = categorize_error(exception)
        
        if context is None:
            context = LogContext()
        
        context.error_category = category
        context.is_retryable = is_retryable
        
        # Include exception details in extra data
        if context.extra_data is None:
            context.extra_data = {}
        context.extra_data["exception_type"] = type(exception).__name__
        context.extra_data["exception_details"] = str(exception)
        
        self._log_with_context(logging.ERROR, message, context, exc_info=True, **kwargs)

    def warning_with_exception(self, message: str, exception: Exception, context: Optional[LogContext] = None, **kwargs):
        """
        Log warning message with automatic error categorization.
        
        Args:
            message: The main warning message
            exception: The exception to categorize
            context: Optional log context
            **kwargs: Additional logging arguments
        """
        category, is_retryable = categorize_error(exception)
        
        if context is None:
            context = LogContext()
        
        context.error_category = category
        context.is_retryable = is_retryable
        
        # Include exception details in extra data
        if context.extra_data is None:
            context.extra_data = {}
        context.extra_data["exception_type"] = type(exception).__name__
        context.extra_data["exception_details"] = str(exception)
        
        self._log_with_context(logging.WARNING, message, context, **kwargs)


def format_structured_message(message: str, context: Dict[str, Any]) -> str:
    """
    Format a log message with structured context.

    Args:
        message: The main log message
        context: Dictionary of contextual key-value pairs

    Returns:
        Formatted message with context information
    """
    if not context:
        return message

    context_parts = []

    for key, value in context.items():
        # Handle complex values by JSON encoding them
        if isinstance(value, (dict, list)):
            try:
                formatted_value = json.dumps(value, separators=(",", ":"))
            except (TypeError, ValueError):
                formatted_value = str(value)
        else:
            formatted_value = str(value)

        context_parts.append(f"{key}={formatted_value}")

    context_str = " ".join(context_parts)
    return f"{message} | {context_str}"


# Convenience function for creating structured loggers
def get_structured_logger(
    name: str, default_context: Optional[Dict[str, Any]] = None
) -> StructuredLogger:
    """
    Create a structured logger with optional default context.

    Args:
        name: Logger name
        default_context: Default context for all log messages

    Returns:
        StructuredLogger instance
    """
    return StructuredLogger(name, default_context)


class JsonFormatter(logging.Formatter):
    """
    Formats log records as JSON.
    """
    def __init__(self, kst_tz):
        super().__init__()
        self.kst_tz = kst_tz

    def format(self, record: logging.LogRecord) -> str:
        """
        Formats a log record into a JSON string.
        """
        log_data = {
            "timestamp": datetime.fromtimestamp(record.created, self.kst_tz).isoformat(),
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


