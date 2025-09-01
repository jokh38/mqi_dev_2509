"""
Retry policy implementation for handling transient failures.
Provides exponential backoff and error categorization.
"""

import time
import logging
import socket
import subprocess
from typing import Any, Callable
from functools import wraps

from src.common.structured_logging import get_structured_logger, LogContext
from src.common.error_categorization import (
    TransientError,
    PermanentError,
    categorize_error,
)

logger = get_structured_logger(__name__)


class RetryExhaustedError(Exception):
    """Exception raised when all retry attempts have been exhausted."""

    pass


class RetryPolicy:
    """
    Implements retry logic with exponential backoff for transient failures.

    Automatically classifies common network and system exceptions as transient
    or permanent errors.
    """

    def __init__(
        self,
        max_retries: int = 3,
        base_delay: float = 1.0,
        max_delay: float = 60.0,
        backoff_multiplier: float = 2.0,
    ):
        """
        Initialize retry policy.

        Args:
            max_retries: Maximum number of retry attempts
            base_delay: Initial delay in seconds
            max_delay: Maximum delay between retries in seconds
            backoff_multiplier: Multiplier for exponential backoff
        """
        self.max_retries = max_retries
        self.base_delay = base_delay
        self.max_delay = max_delay
        self.backoff_multiplier = backoff_multiplier

    def _calculate_delay(self, attempt: int) -> float:
        """Calculate delay for the given attempt number."""
        delay = self.base_delay * (self.backoff_multiplier**attempt)
        return min(delay, self.max_delay)

    def _is_transient_error(self, exception: Exception) -> bool:
        """
        Determine if an exception should be treated as transient.

        Args:
            exception: The exception to classify

        Returns:
            True if the exception is transient and should be retried
        """
        _, is_retryable = categorize_error(exception)
        return is_retryable

    def execute(self, func: Callable, *args, **kwargs) -> Any:
        """
        Execute a function with retry logic.

        Args:
            func: The function to execute
            *args: Positional arguments for the function
            **kwargs: Keyword arguments for the function

        Returns:
            The result of the successful function call

        Raises:
            PermanentError: For errors that should not be retried
            RetryExhaustedError: When all retry attempts are exhausted
        """
        last_exception = None

        for attempt in range(self.max_retries + 1):  # +1 for initial attempt
            try:
                result = func(*args, **kwargs)
                if attempt > 0:
                    logger.info(
                        "Function succeeded after retries",
                        context=LogContext(
                            operation=func.__name__,
                            extra_data={
                                "category": "retry_success",
                                "retry_attempt": attempt,
                                "total_attempts": attempt + 1,
                            },
                        ),
                    )
                return result

            except Exception as e:
                last_exception = e

                if not self._is_transient_error(e):
                    logger.error_with_exception(
                        "Permanent error encountered - not retrying",
                        e,
                        context=LogContext(
                            operation=func.__name__,
                            extra_data={
                                "category": "retry_permanent_error",
                                "error_type": type(e).__name__,
                                "retry_attempt": attempt + 1,
                            },
                        ),
                    )
                    raise e

                if attempt < self.max_retries:
                    delay = self._calculate_delay(attempt)
                    logger.warning_with_exception(
                        "Transient error occurred - retrying after delay",
                        e,
                        context=LogContext(
                            operation=func.__name__,
                            extra_data={
                                "category": "retry_transient_error",
                                "error_type": type(e).__name__,
                                "retry_attempt": attempt + 1,
                                "max_retries": self.max_retries + 1,
                                "retry_delay_seconds": delay,
                                "is_transient": True,
                            },
                        ),
                    )
                    time.sleep(delay)
                else:
                    logger.error(
                        "Exhausted all retry attempts",
                        context=LogContext(
                            operation=func.__name__,
                            extra_data={
                                "category": "retry_exhausted",
                                "max_retries": self.max_retries,
                                "total_attempts": self.max_retries + 1,
                                "final_error_type": type(last_exception).__name__
                                if last_exception
                                else "unknown",
                            },
                        ),
                    )

        # If we get here, all attempts failed
        raise RetryExhaustedError(
            f"Failed after {self.max_retries + 1} attempts. Last error: {last_exception}"
        ) from last_exception


def with_retry(
    max_retries: int = 3,
    base_delay: float = 1.0,
    max_delay: float = 60.0,
    backoff_multiplier: float = 2.0,
):
    """
    Decorator for applying retry logic to functions.

    Args:
        max_retries: Maximum number of retry attempts
        base_delay: Initial delay in seconds
        max_delay: Maximum delay between retries in seconds
        backoff_multiplier: Multiplier for exponential backoff
    """

    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            policy = RetryPolicy(max_retries, base_delay, max_delay, backoff_multiplier)
            return policy.execute(func, *args, **kwargs)

        return wrapper

    return decorator
