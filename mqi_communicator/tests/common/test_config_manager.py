"""
Tests for the configuration management module.
"""

import os
import tempfile
import pytest
import yaml
from src.common.config_manager import ConfigManager, ConfigValidationError


class TestConfigManager:
    """Test suite for ConfigManager class."""

    def setup_method(self):
        """Set up test fixtures before each test method."""
        self.valid_config = {
            "logging": {"path": "communicator_local.log"},
            "database": {"path": "database/mqi_communicator.db"},
            "dashboard": {"auto_start": True},
            "hpc": {
                "host": "10.243.62.128",
                "user": "jokh38",
                "remote_base_dir": "~/MOQUI_SMC",
                "remote_command": "python interpreter.py && python moquisim.py",
                "scp_command": "scp",
                "ssh_command": "ssh",
                "pueue_command": "pueue",
            },
            "scanner": {
                "watch_path": "new_cases",
                "quiescence_period_seconds": 5,
            },
            "main_loop": {
                "sleep_interval_seconds": 10,
                "running_case_timeout_hours": 24,
            },
            "pueue": {
                "groups": ["default", "gpu_a", "gpu_b"],
            },
        }

    def create_temp_config_file(self, config_dict):
        """Create a temporary configuration file with the given config."""
        temp_file = tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False)
        yaml.dump(config_dict, temp_file)
        temp_file.close()
        return temp_file.name

    def test_load_valid_config_succeeds(self):
        """Test that loading a valid configuration succeeds."""
        config_path = self.create_temp_config_file(self.valid_config)
        try:
            config_manager = ConfigManager(config_path)
            assert config_manager.get("database.path") == "database/mqi_communicator.db"
            assert config_manager.get("hpc.host") == "10.243.62.128"
            assert config_manager.get("pueue.groups") == ["default", "gpu_a", "gpu_b"]
        finally:
            os.unlink(config_path)

    def test_load_config_with_missing_file_raises_error(self):
        """Test that loading non-existent config file raises ConfigValidationError."""
        with pytest.raises(ConfigValidationError, match="Config file not found"):
            ConfigManager("/nonexistent/config.yaml")

    def test_load_config_with_invalid_yaml_raises_error(self):
        """Test that loading malformed YAML raises ConfigValidationError."""
        temp_file = tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False)
        temp_file.write("invalid: yaml: content: [")
        temp_file.close()

        try:
            with pytest.raises(ConfigValidationError, match="Invalid YAML format"):
                ConfigManager(temp_file.name)
        finally:
            os.unlink(temp_file.name)

    def test_validate_config_with_missing_required_section_fails(self):
        """Test that missing required sections cause validation to fail."""
        invalid_config = self.valid_config.copy()
        del invalid_config["hpc"]

        config_path = self.create_temp_config_file(invalid_config)
        try:
            with pytest.raises(
                ConfigValidationError, match="Missing required section: hpc"
            ):
                ConfigManager(config_path)
        finally:
            os.unlink(config_path)

    def test_validate_config_with_missing_required_field_fails(self):
        """Test that missing required fields cause validation to fail."""
        invalid_config = self.valid_config.copy()
        del invalid_config["hpc"]["host"]

        config_path = self.create_temp_config_file(invalid_config)
        try:
            with pytest.raises(
                ConfigValidationError, match="Missing required field: hpc.host"
            ):
                ConfigManager(config_path)
        finally:
            os.unlink(config_path)

    def test_validate_config_with_invalid_type_fails(self):
        """Test that invalid field types cause validation to fail."""
        invalid_config = self.valid_config.copy()
        invalid_config["scanner"]["quiescence_period_seconds"] = "not_a_number"

        config_path = self.create_temp_config_file(invalid_config)
        try:
            with pytest.raises(
                ConfigValidationError,
                match="Invalid type for scanner.quiescence_period_seconds",
            ):
                ConfigManager(config_path)
        finally:
            os.unlink(config_path)

    def test_get_with_dot_notation_succeeds(self):
        """Test that dot notation access works correctly."""
        config_path = self.create_temp_config_file(self.valid_config)
        try:
            config_manager = ConfigManager(config_path)
            assert config_manager.get("hpc.host") == "10.243.62.128"
            assert config_manager.get("scanner.quiescence_period_seconds") == 5
            assert config_manager.get("pueue.groups") == ["default", "gpu_a", "gpu_b"]
        finally:
            os.unlink(config_path)

    def test_get_with_default_value_returns_default_when_missing(self):
        """Test that get method returns default value when key is missing."""
        config_path = self.create_temp_config_file(self.valid_config)
        try:
            config_manager = ConfigManager(config_path)
            assert (
                config_manager.get("nonexistent.key", "default_value")
                == "default_value"
            )
        finally:
            os.unlink(config_path)

    def test_get_without_default_raises_error_when_missing(self):
        """Test that get method raises error when key is missing and no default provided."""
        config_path = self.create_temp_config_file(self.valid_config)
        try:
            config_manager = ConfigManager(config_path)
            with pytest.raises(
                ConfigValidationError,
                match="Configuration key not found: nonexistent.key",
            ):
                config_manager.get("nonexistent.key")
        finally:
            os.unlink(config_path)

    def test_config_with_default_values_applied(self):
        """Test that default values are applied for optional fields."""
        minimal_config = {
            "database": {"path": "test.db"},
            "hpc": {
                "host": "test.host",
                "user": "testuser",
                "remote_base_dir": "~/test",
                "remote_command": "test command",
            },
            "scanner": {"watch_path": "test_cases"},
            "main_loop": {},
            "pueue": {"groups": ["default"]},
        }

        config_path = self.create_temp_config_file(minimal_config)
        try:
            config_manager = ConfigManager(config_path)
            # Check that defaults are applied
            assert config_manager.get("dashboard.auto_start") is True
            assert config_manager.get("hpc.scp_command") == "scp"
            assert config_manager.get("hpc.ssh_command") == "ssh"
            assert config_manager.get("hpc.pueue_command") == "pueue"
            assert config_manager.get("scanner.quiescence_period_seconds") == 5
            assert config_manager.get("main_loop.sleep_interval_seconds") == 10
            assert config_manager.get("main_loop.running_case_timeout_hours") == 24
        finally:
            os.unlink(config_path)

    def test_get_section_returns_entire_section(self):
        """Test that get_section method returns entire configuration sections."""
        config_path = self.create_temp_config_file(self.valid_config)
        try:
            config_manager = ConfigManager(config_path)
            hpc_section = config_manager.get_section("hpc")
            assert hpc_section["host"] == "10.243.62.128"
            assert hpc_section["user"] == "jokh38"
            assert hpc_section["remote_base_dir"] == "~/MOQUI_SMC"
        finally:
            os.unlink(config_path)

    def test_get_section_with_missing_section_raises_error(self):
        """Test that get_section raises error when section doesn't exist."""
        config_path = self.create_temp_config_file(self.valid_config)
        try:
            config_manager = ConfigManager(config_path)
            with pytest.raises(
                ConfigValidationError,
                match="Configuration section not found: nonexistent",
            ):
                config_manager.get_section("nonexistent")
        finally:
            os.unlink(config_path)
