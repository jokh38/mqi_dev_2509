"""
Test cases for StructuredLogger functionality.
Following TDD principles - these tests should fail initially.
"""

import logging
from unittest.mock import Mock, patch
from src.common.structured_logging import (
    LogContext,
    StructuredLogger,
    format_structured_message,
)


class TestLogContext:
    """Test suite for LogContext class."""

    def test_log_context_initialization(self):
        """Test that LogContext initializes correctly."""
        context = LogContext(case_id="case_123", operation="submit_workflow")
        assert context.case_id == "case_123"
        assert context.operation == "submit_workflow"
        assert context.extra_data == {}

    def test_log_context_with_extra_data(self):
        """Test that LogContext accepts extra data."""
        extra = {"retry_count": 2, "gpu_group": "gpu_a"}
        context = LogContext(case_id="case_123", extra_data=extra)
        assert context.extra_data == extra

    def test_log_context_to_dict(self):
        """Test conversion of LogContext to dictionary."""
        context = LogContext(
            case_id="case_123",
            operation="transfer_files",
            extra_data={"file_count": 5, "size_mb": 120},
        )

        expected_dict = {
            "case_id": "case_123",
            "operation": "transfer_files",
            "file_count": 5,
            "size_mb": 120,
        }

        assert context.to_dict() == expected_dict


class TestStructuredLogger:
    """Test suite for StructuredLogger class."""

    def test_structured_logger_initialization(self):
        """Test that StructuredLogger initializes correctly."""
        logger = StructuredLogger("test_logger")
        assert logger.logger.name == "test_logger"
        assert logger.default_context == {}

    def test_structured_logger_with_default_context(self):
        """Test initialization with default context."""
        default_context = {"service": "workflow_submitter", "version": "1.0"}
        logger = StructuredLogger("test_logger", default_context)
        assert logger.default_context == default_context

    @patch("src.common.structured_logging.logging.getLogger")
    def test_info_with_context(self, mock_get_logger):
        """Test info logging with context."""
        mock_logger = Mock()
        mock_get_logger.return_value = mock_logger

        structured_logger = StructuredLogger("test")
        context = LogContext(case_id="case_123", operation="test_op")

        structured_logger.info("Test message", context)

        # Verify the logger was called with structured message
        mock_logger.log.assert_called_once()
        call_args = mock_logger.log.call_args[0]
        assert call_args[0] == logging.INFO  # Log level
        message = call_args[1]  # Log message
        assert "case_id" in message
        assert "operation" in message
        assert "Test message" in message

    @patch("src.common.structured_logging.logging.getLogger")
    def test_error_with_exception_context(self, mock_get_logger):
        """Test error logging with exception context."""
        mock_logger = Mock()
        mock_get_logger.return_value = mock_logger

        structured_logger = StructuredLogger("test")

        try:
            raise ValueError("Test exception")
        except ValueError as e:
            context = LogContext(
                case_id="case_123",
                operation="test_op",
                extra_data={"error_type": type(e).__name__},
            )
            structured_logger.error("Operation failed", context, exc_info=True)

        mock_logger.log.assert_called_once()
        call_args, call_kwargs = mock_logger.log.call_args
        assert call_args[0] == logging.ERROR  # Log level
        message = call_args[1]  # Log message
        assert "case_id" in message
        assert "error_type" in message
        assert call_kwargs.get("exc_info") is True

    @patch("src.common.structured_logging.logging.getLogger")
    def test_warning_with_performance_metrics(self, mock_get_logger):
        """Test warning logging with performance context."""
        mock_logger = Mock()
        mock_get_logger.return_value = mock_logger

        structured_logger = StructuredLogger("test")
        context = LogContext(
            case_id="case_123",
            operation="file_transfer",
            extra_data={
                "duration_seconds": 45.5,
                "file_size_mb": 200,
                "transfer_rate_mbps": 4.4,
            },
        )

        structured_logger.warning("Slow transfer detected", context)

        mock_logger.log.assert_called_once()
        call_args = mock_logger.log.call_args[0]
        assert call_args[0] == logging.WARNING  # Log level
        message = call_args[1]  # Log message
        assert "duration_seconds" in message
        assert "transfer_rate_mbps" in message

    @patch("src.common.structured_logging.logging.getLogger")
    def test_default_context_merging(self, mock_get_logger):
        """Test that default context is merged with specific context."""
        mock_logger = Mock()
        mock_get_logger.return_value = mock_logger

        default_context = {"service": "workflow_submitter", "host": "hpc01"}
        structured_logger = StructuredLogger("test", default_context)

        context = LogContext(case_id="case_123", operation="submit")
        structured_logger.info("Test message", context)

        mock_logger.log.assert_called_once()
        call_args = mock_logger.log.call_args[0]
        assert call_args[0] == logging.INFO  # Log level
        message = call_args[1]  # Log message
        assert "service" in message
        assert "host" in message
        assert "case_id" in message
        assert "operation" in message


class TestFormatStructuredMessage:
    """Test suite for format_structured_message function."""

    def test_format_with_basic_context(self):
        """Test formatting with basic context."""
        context = {"case_id": "case_123", "operation": "submit"}
        message = "Job submitted successfully"

        result = format_structured_message(message, context)

        assert "case_id=case_123" in result
        assert "operation=submit" in result
        assert "Job submitted successfully" in result

    def test_format_with_complex_context(self):
        """Test formatting with complex context data."""
        context = {
            "case_id": "case_123",
            "operation": "transfer",
            "metrics": {"duration": 30.5, "file_count": 15},
            "status": "completed",
        }
        message = "Transfer completed"

        result = format_structured_message(message, context)

        # Should contain serialized metrics as JSON
        assert "case_id=case_123" in result
        assert "metrics=" in result
        assert "duration" in result
        assert "file_count" in result

    def test_format_handles_none_values(self):
        """Test that None values are handled gracefully."""
        context = {"case_id": "case_123", "gpu_group": None}
        message = "Processing case"

        result = format_structured_message(message, context)

        assert "case_id=case_123" in result
        assert "gpu_group=None" in result

    def test_format_handles_special_characters(self):
        """Test handling of special characters in values."""
        context = {
            "case_id": "case with spaces",
            "path": "/path/with spaces/file.txt",
            "message": "Error: Connection failed!",
        }
        message = "Processing failed"

        result = format_structured_message(message, context)

        # Should handle special characters without breaking format
        assert "case_id=" in result
        assert "path=" in result
        assert "message=" in result
