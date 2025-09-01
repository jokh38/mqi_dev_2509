"""
Test cases for Error Categorization functionality.
Following TDD principles - these tests should fail initially.
"""

import socket
import subprocess
from src.common.error_categorization import (
    ErrorCategory,
    ErrorClassifier,
    categorize_error,
    NetworkError,
    SystemError,
    ConfigurationError,
    ApplicationError,
)


class TestErrorCategory:
    """Test suite for ErrorCategory enum."""

    def test_error_category_values(self):
        """Test that ErrorCategory enum has expected values."""
        assert ErrorCategory.NETWORK.value == "network"
        assert ErrorCategory.SYSTEM.value == "system"
        assert ErrorCategory.CONFIGURATION.value == "configuration"
        assert ErrorCategory.APPLICATION.value == "application"
        assert ErrorCategory.UNKNOWN.value == "unknown"

    def test_error_category_is_retryable(self):
        """Test retryable categorization of error categories."""
        # Network and system errors are typically retryable
        assert ErrorCategory.NETWORK.is_retryable() is True
        assert ErrorCategory.SYSTEM.is_retryable() is True

        # Configuration and application errors are typically not retryable
        assert ErrorCategory.CONFIGURATION.is_retryable() is False
        assert ErrorCategory.APPLICATION.is_retryable() is False

        # Unknown errors default to not retryable for safety
        assert ErrorCategory.UNKNOWN.is_retryable() is False


class TestErrorClassifier:
    """Test suite for ErrorClassifier class."""

    def test_classifier_initialization(self):
        """Test that ErrorClassifier initializes correctly."""
        classifier = ErrorClassifier()
        assert classifier is not None

    def test_classify_network_errors(self):
        """Test classification of network-related errors."""
        classifier = ErrorClassifier()

        network_errors = [
            socket.timeout("Connection timeout"),
            socket.gaierror("Name resolution failed"),
            ConnectionRefusedError("Connection refused"),
            ConnectionResetError("Connection reset"),
            ConnectionAbortedError("Connection aborted"),
        ]

        for error in network_errors:
            category = classifier.classify(error)
            assert category == ErrorCategory.NETWORK

    def test_classify_system_errors(self):
        """Test classification of system-related errors."""
        classifier = ErrorClassifier()

        system_errors = [
            subprocess.TimeoutExpired("cmd", 30),
            PermissionError("Permission denied"),
            OSError("System error"),
            FileNotFoundError("File not found"),
        ]

        for error in system_errors:
            category = classifier.classify(error)
            assert category == ErrorCategory.SYSTEM

    def test_classify_custom_error_types(self):
        """Test classification of custom error types."""
        classifier = ErrorClassifier()

        # Test custom error types
        network_error = NetworkError("Custom network issue")
        system_error = SystemError("Custom system issue")
        config_error = ConfigurationError("Invalid config")
        app_error = ApplicationError("Business logic error")

        assert classifier.classify(network_error) == ErrorCategory.NETWORK
        assert classifier.classify(system_error) == ErrorCategory.SYSTEM
        assert classifier.classify(config_error) == ErrorCategory.CONFIGURATION
        assert classifier.classify(app_error) == ErrorCategory.APPLICATION

    def test_classify_subprocess_errors_with_return_codes(self):
        """Test classification of subprocess errors based on return codes."""
        classifier = ErrorClassifier()

        # Network-related return codes (e.g., SSH connection failures)
        network_subprocess_error = subprocess.CalledProcessError(
            255, "ssh", "SSH connection failed"
        )
        assert classifier.classify(network_subprocess_error) == ErrorCategory.NETWORK

        # System-related return codes (e.g., permission denied)
        system_subprocess_error = subprocess.CalledProcessError(
            126, "chmod", "Permission denied"
        )
        assert classifier.classify(system_subprocess_error) == ErrorCategory.SYSTEM

        # Application-related return codes
        app_subprocess_error = subprocess.CalledProcessError(
            1, "python", "Python script error"
        )
        assert classifier.classify(app_subprocess_error) == ErrorCategory.APPLICATION

    def test_classify_unknown_errors(self):
        """Test classification of unknown error types."""
        classifier = ErrorClassifier()

        # Custom exception that doesn't match known patterns
        class UnknownError(Exception):
            pass

        unknown_error = UnknownError("Mystery error")
        category = classifier.classify(unknown_error)
        assert category == ErrorCategory.UNKNOWN

    def test_classify_error_by_message_patterns(self):
        """Test classification based on error message patterns."""
        classifier = ErrorClassifier()

        # Generic exceptions with network-related messages
        network_generic = Exception("Connection timed out")
        assert classifier.classify(network_generic) == ErrorCategory.NETWORK

        # Generic exceptions with config-related messages
        config_generic = Exception("Invalid configuration file")
        assert classifier.classify(config_generic) == ErrorCategory.CONFIGURATION

        # Generic exceptions with unknown messages
        unknown_generic = Exception("Something went wrong")
        assert classifier.classify(unknown_generic) == ErrorCategory.UNKNOWN


class TestCategorizeErrorFunction:
    """Test suite for categorize_error convenience function."""

    def test_categorize_error_returns_tuple(self):
        """Test that categorize_error returns (category, is_retryable) tuple."""
        error = socket.timeout("Connection timeout")
        category, is_retryable = categorize_error(error)

        assert isinstance(category, ErrorCategory)
        assert isinstance(is_retryable, bool)
        assert category == ErrorCategory.NETWORK
        assert is_retryable is True

    def test_categorize_error_with_various_errors(self):
        """Test categorize_error with different error types."""
        test_cases = [
            (socket.timeout("Timeout"), ErrorCategory.NETWORK, True),
            (PermissionError("Access denied"), ErrorCategory.SYSTEM, True),
            (ConfigurationError("Bad config"), ErrorCategory.CONFIGURATION, False),
            (ValueError("Invalid value"), ErrorCategory.UNKNOWN, False),
        ]

        for error, expected_category, expected_retryable in test_cases:
            category, is_retryable = categorize_error(error)
            assert category == expected_category
            assert is_retryable == expected_retryable


class TestCustomErrorTypes:
    """Test suite for custom error types."""

    def test_network_error_creation(self):
        """Test NetworkError custom exception."""
        error = NetworkError("Connection failed", details={"host": "hpc01", "port": 22})
        assert str(error) == "Connection failed"
        assert error.details == {"host": "hpc01", "port": 22}

    def test_system_error_creation(self):
        """Test SystemError custom exception."""
        error = SystemError("Disk full", details={"usage": "95%"})
        assert str(error) == "Disk full"
        assert error.details == {"usage": "95%"}

    def test_configuration_error_creation(self):
        """Test ConfigurationError custom exception."""
        error = ConfigurationError("Missing required field", field="database.path")
        assert str(error) == "Missing required field"
        assert error.field == "database.path"

    def test_application_error_creation(self):
        """Test ApplicationError custom exception."""
        error = ApplicationError("Business logic failure", operation="process_case")
        assert str(error) == "Business logic failure"
        assert error.operation == "process_case"
