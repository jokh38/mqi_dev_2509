"""
Test cases for RetryPolicy functionality.
Following TDD principles - these tests should fail initially.
"""

import pytest
from unittest.mock import Mock, patch
from src.common.retry_policy import (
    RetryPolicy,
    RetryExhaustedError,
)
from src.common.error_categorization import (
    NetworkError,
    ConfigurationError,
)


class TestRetryPolicy:
    """Test suite for RetryPolicy class."""

    def test_retry_policy_initialization(self):
        """Test that RetryPolicy initializes with correct defaults."""
        policy = RetryPolicy()
        assert policy.max_retries == 3
        assert policy.base_delay == 1.0
        assert policy.max_delay == 60.0
        assert policy.backoff_multiplier == 2.0

    def test_retry_policy_custom_initialization(self):
        """Test that RetryPolicy accepts custom parameters."""
        policy = RetryPolicy(
            max_retries=5, base_delay=0.5, max_delay=30.0, backoff_multiplier=1.5
        )
        assert policy.max_retries == 5
        assert policy.base_delay == 0.5
        assert policy.max_delay == 30.0
        assert policy.backoff_multiplier == 1.5

    def test_retry_succeeds_on_first_attempt(self):
        """Test that successful functions execute without retry."""
        policy = RetryPolicy()
        mock_func = Mock(return_value="success")

        result = policy.execute(mock_func)

        assert result == "success"
        assert mock_func.call_count == 1

    def test_retry_succeeds_after_transient_failure(self):
        """Test that functions succeed after initial transient failures."""
        policy = RetryPolicy(max_retries=3)
        mock_func = Mock(
            side_effect=[
                NetworkError("Network timeout"),
                NetworkError("Connection refused"),
                "success",
            ]
        )

        result = policy.execute(mock_func)

        assert result == "success"
        assert mock_func.call_count == 3

    def test_retry_fails_with_permanent_error(self):
        """Test that permanent errors are not retried."""
        policy = RetryPolicy()
        mock_func = Mock(side_effect=ConfigurationError("Invalid configuration"))

        with pytest.raises(ConfigurationError):
            policy.execute(mock_func)

        assert mock_func.call_count == 1

    def test_retry_exhausts_attempts_with_transient_error(self):
        """Test that retry exhaustion raises appropriate error."""
        policy = RetryPolicy(max_retries=2)
        mock_func = Mock(side_effect=NetworkError("Always fails"))

        with pytest.raises(RetryExhaustedError):
            policy.execute(mock_func)

        assert mock_func.call_count == 3  # Initial + 2 retries

    @patch("time.sleep")
    def test_exponential_backoff_delays(self, mock_sleep):
        """Test that retry delays follow exponential backoff."""
        policy = RetryPolicy(max_retries=3, base_delay=1.0, backoff_multiplier=2.0)
        mock_func = Mock(
            side_effect=[
                NetworkError("Fail 1"),
                NetworkError("Fail 2"),
                NetworkError("Fail 3"),
                NetworkError("Fail 4"),
            ]
        )

        with pytest.raises(RetryExhaustedError):
            policy.execute(mock_func)

        # Verify exponential backoff: 1.0, 2.0, 4.0
        expected_delays = [1.0, 2.0, 4.0]
        actual_delays = [call[0][0] for call in mock_sleep.call_args_list]
        assert actual_delays == expected_delays

    @patch("time.sleep")
    def test_max_delay_cap(self, mock_sleep):
        """Test that delay is capped at max_delay."""
        policy = RetryPolicy(
            max_retries=5, base_delay=10.0, max_delay=15.0, backoff_multiplier=2.0
        )
        mock_func = Mock(side_effect=NetworkError("Always fails"))

        with pytest.raises(RetryExhaustedError):
            policy.execute(mock_func)

        # All delays should be capped at 15.0
        actual_delays = [call[0][0] for call in mock_sleep.call_args_list]
        for delay in actual_delays:
            assert delay <= 15.0

    def test_retry_with_function_arguments(self):
        """Test that function arguments are preserved during retries."""
        policy = RetryPolicy(max_retries=2)
        mock_func = Mock(side_effect=[NetworkError("Fail once"), "success with args"])

        result = policy.execute(mock_func, "arg1", "arg2", kwarg1="value1")

        assert result == "success with args"
        mock_func.assert_called_with("arg1", "arg2", kwarg1="value1")
        assert mock_func.call_count == 2

    def test_transient_error_detection_for_network_issues(self):
        """Test that network-related exceptions are classified as transient."""
        policy = RetryPolicy()

        # Test various network exceptions that should be treated as transient
        import socket
        import subprocess

        transient_exceptions = [
            socket.timeout("Connection timeout"),
            subprocess.TimeoutExpired("cmd", 30),
            ConnectionRefusedError("Connection refused"),
            ConnectionResetError("Connection reset"),
        ]

        for exception in transient_exceptions:
            mock_func = Mock(side_effect=[exception, "success"])
            result = policy.execute(mock_func)
            assert result == "success"
            assert mock_func.call_count == 2
            mock_func.reset_mock()
