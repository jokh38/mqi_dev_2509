"""
Error categorization system for enhanced error handling and observability.
Categorizes errors into types and determines retry behavior.
"""

import re
import subprocess
import socket
from enum import Enum
from typing import Any, Dict, Optional, Tuple


class ErrorCategory(Enum):
    """
    Categories for error classification.

    Used to determine error handling behavior and retry policies.
    """

    NETWORK = "network"
    SYSTEM = "system"
    CONFIGURATION = "configuration"
    APPLICATION = "application"
    UNKNOWN = "unknown"

    def is_retryable(self) -> bool:
        """
        Determine if errors of this category should be retried.

        Returns:
            True if errors of this category are typically transient and retryable
        """
        # Network and system errors are often transient and retryable
        retryable_categories = {self.NETWORK, self.SYSTEM}
        return self in retryable_categories


class BaseExecutionError(Exception):
    """Base class for all execution-related errors with standardized interface."""
    
    def __init__(self, message: str, details: Optional[Dict[str, Any]] = None):
        super().__init__(message)
        self.details = details or {}


class NetworkError(Exception):
    """Custom exception for network-related errors."""

    def __init__(self, message: str, details: Optional[Dict[str, Any]] = None):
        super().__init__(message)
        self.details = details or {}


class SystemError(Exception):
    """Custom exception for system-related errors."""

    def __init__(self, message: str, details: Optional[Dict[str, Any]] = None):
        super().__init__(message)
        self.details = details or {}


class ConfigurationError(Exception):
    """Custom exception for configuration-related errors."""

    def __init__(self, message: str, field: Optional[str] = None):
        super().__init__(message)
        self.field = field


class ApplicationError(Exception):
    """Custom exception for application logic errors."""

    def __init__(self, message: str, operation: Optional[str] = None):
        super().__init__(message)
        self.operation = operation


class ErrorClassifier:
    """
    Classifies errors into categories for appropriate handling.

    Uses exception types, return codes, and message patterns to determine
    the appropriate error category and handling strategy.
    """

    # Exception type to category mapping
    TYPE_CATEGORY_MAP = {
        # Network-related exceptions
        socket.timeout: ErrorCategory.NETWORK,
        socket.gaierror: ErrorCategory.NETWORK,
        socket.herror: ErrorCategory.NETWORK,
        ConnectionError: ErrorCategory.NETWORK,
        ConnectionRefusedError: ErrorCategory.NETWORK,
        ConnectionResetError: ErrorCategory.NETWORK,
        ConnectionAbortedError: ErrorCategory.NETWORK,
        NetworkError: ErrorCategory.NETWORK,
        # System-related exceptions
        subprocess.TimeoutExpired: ErrorCategory.SYSTEM,
        PermissionError: ErrorCategory.SYSTEM,
        OSError: ErrorCategory.SYSTEM,
        IOError: ErrorCategory.SYSTEM,
        FileNotFoundError: ErrorCategory.SYSTEM,
        FileExistsError: ErrorCategory.SYSTEM,
        SystemError: ErrorCategory.SYSTEM,
        # Configuration-related exceptions
        ConfigurationError: ErrorCategory.CONFIGURATION,
        # Application-related exceptions
        ApplicationError: ErrorCategory.APPLICATION,
    }

    # Return code ranges for subprocess errors
    SUBPROCESS_RETURN_CODE_CATEGORIES = {
        # Network/connectivity issues (SSH, SCP failures)
        range(255, 256): ErrorCategory.NETWORK,  # SSH connection failures
        range(254, 255): ErrorCategory.NETWORK,  # SSH protocol errors
        # System/permission issues
        range(
            126, 128
        ): ErrorCategory.SYSTEM,  # Permission denied, command not executable
        range(2, 3): ErrorCategory.SYSTEM,  # File not found
        # Application logic issues (general failures)
        range(1, 2): ErrorCategory.APPLICATION,  # General errors
    }

    # Message patterns for classification
    MESSAGE_PATTERNS = [
        # Network patterns
        (
            re.compile(
                r"connection.*tim[ed].*out|timeout.*connection|connection.*timed.*out",
                re.IGNORECASE,
            ),
            ErrorCategory.NETWORK,
        ),
        (
            re.compile(r"connection.*refused|refused.*connection", re.IGNORECASE),
            ErrorCategory.NETWORK,
        ),
        (
            re.compile(r"connection.*reset|reset.*connection", re.IGNORECASE),
            ErrorCategory.NETWORK,
        ),
        (
            re.compile(r"network.*unreachable|unreachable.*network", re.IGNORECASE),
            ErrorCategory.NETWORK,
        ),
        (
            re.compile(r"host.*unreachable|unreachable.*host", re.IGNORECASE),
            ErrorCategory.NETWORK,
        ),
        (
            re.compile(r"name.*resolution.*failed|dns.*failed", re.IGNORECASE),
            ErrorCategory.NETWORK,
        ),
        # System patterns
        (
            re.compile(r"permission.*denied|access.*denied", re.IGNORECASE),
            ErrorCategory.SYSTEM,
        ),
        (
            re.compile(r"file.*not.*found|no.*such.*file", re.IGNORECASE),
            ErrorCategory.SYSTEM,
        ),
        (
            re.compile(r"disk.*full|no.*space.*left", re.IGNORECASE),
            ErrorCategory.SYSTEM,
        ),
        (
            re.compile(r"out.*of.*memory|memory.*exhausted", re.IGNORECASE),
            ErrorCategory.SYSTEM,
        ),
        # Configuration patterns
        (
            re.compile(
                r"invalid.*config|config.*error|configuration.*error", re.IGNORECASE
            ),
            ErrorCategory.CONFIGURATION,
        ),
        (
            re.compile(r"missing.*required|required.*missing", re.IGNORECASE),
            ErrorCategory.CONFIGURATION,
        ),
        (
            re.compile(r"invalid.*format|format.*invalid", re.IGNORECASE),
            ErrorCategory.CONFIGURATION,
        ),
    ]

    def __init__(self):
        """Initialize the error classifier."""
        pass

    def classify(self, error: Exception) -> ErrorCategory:
        """
        Classify an error into an appropriate category.

        Args:
            error: The exception to classify

        Returns:
            The appropriate ErrorCategory for the error
        """
        # First, try exact type matching
        error_type = type(error)
        if error_type in self.TYPE_CATEGORY_MAP:
            return self.TYPE_CATEGORY_MAP[error_type]

        # Check parent types for inheritance
        for exc_type, category in self.TYPE_CATEGORY_MAP.items():
            if isinstance(error, exc_type):
                return category

        # Special handling for subprocess.CalledProcessError
        if isinstance(error, subprocess.CalledProcessError):
            return self._classify_subprocess_error(error)

        # Try message pattern matching
        error_message = str(error)
        for pattern, category in self.MESSAGE_PATTERNS:
            if pattern.search(error_message):
                return category

        # Default to unknown if no classification matches
        return ErrorCategory.UNKNOWN

    def _classify_subprocess_error(
        self, error: subprocess.CalledProcessError
    ) -> ErrorCategory:
        """
        Classify subprocess errors based on return codes and context.

        Args:
            error: The subprocess.CalledProcessError to classify

        Returns:
            The appropriate ErrorCategory
        """
        return_code = error.returncode

        # Check return code ranges
        for code_range, category in self.SUBPROCESS_RETURN_CODE_CATEGORIES.items():
            if return_code in code_range:
                return category

        # Check stderr for additional clues
        if error.stderr:
            stderr_message = str(error.stderr)
            for pattern, category in self.MESSAGE_PATTERNS:
                if pattern.search(stderr_message):
                    return category

        # Default to application error for unknown subprocess failures
        return ErrorCategory.APPLICATION


def categorize_error(error: Exception, context: str = "") -> Tuple[ErrorCategory, bool]:
    """
    Convenience function to categorize an error and determine retry behavior.

    Args:
        error: The exception to categorize
        context: Optional context string for additional error categorization context

    Returns:
        Tuple of (ErrorCategory, is_retryable)
    """
    classifier = ErrorClassifier()
    category = classifier.classify(error)
    is_retryable = category.is_retryable()

    return category, is_retryable
