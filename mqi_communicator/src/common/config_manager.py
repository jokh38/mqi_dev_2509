"""
Configuration management module for the MQI Communicator.

This module provides robust configuration loading, validation, and access
with schema validation and default value handling.
"""

import yaml
from typing import Any, Dict
from pathlib import Path


class ConfigValidationError(Exception):
    """Raised when configuration validation fails."""

    pass


class ConfigManager:
    """
    Manages application configuration with validation and default values.

    Provides dot notation access to configuration values and validates
    configuration against a predefined schema.
    """

    # Configuration schema with required fields and their types
    SCHEMA = {
        "logging": {
            "required": False,
            "fields": {"path": {"type": str, "default": "communicator_local.log"}},
        },
        "database": {
            "required": True,
            "fields": {
                "path": {"type": str, "required": True},
                "enable_cache": {"type": bool, "default": True},
                "cache_size": {"type": int, "default": 1000},
                "cache_ttl_seconds": {"type": int, "default": 300},
                "enable_wal_mode": {"type": bool, "default": True},
                "connection_timeout_seconds": {"type": int, "default": 30},
            },
        },
        "dashboard": {
            "required": False,
            "fields": {"auto_start": {"type": bool, "default": True}},
        },
        "hpc": {
            "required": True,
            "fields": {
                "host": {"type": str, "required": True},
                "user": {"type": str, "required": True},
                "remote_base_dir": {"type": str, "required": True},
                "moqui_interpreter_outputs_dir": {"type": str, "default": "~/Outputs_csv"},
                "moqui_outputs_dir": {"type": str, "default": "~/Dose_raw"},
                "remote_command": {"type": str, "required": False},
                "scp_command": {"type": str, "default": "scp"},
                "ssh_command": {"type": str, "default": "ssh"},
                "pueue_command": {"type": str, "default": "pueue"},
            },
        },
        "scanner": {
            "required": True,
            "fields": {
                "watch_path": {"type": str, "required": True},
                "quiescence_period_seconds": {"type": int, "default": 5},
            },
        },
        "main_loop": {
            "required": True,
            "fields": {
                "sleep_interval_seconds": {"type": int, "default": 10},
                "running_case_timeout_hours": {"type": int, "default": 24},
                "parallel_processing": {
                    "type": dict,
                    "required": False,
                    "default": {
                        "enabled": True,
                        "max_workers": 4,
                        "batch_size": 10,
                        "processing_timeout": 300.0
                    }
                },
                "priority_scheduling": {
                    "type": dict,
                    "required": False,
                    "default": {
                        "enabled": True,
                        "algorithm": "weighted_fair",
                        "aging_factor": 0.1,
                        "starvation_threshold_hours": 24
                    }
                },
                "gpu_refresh_interval_iterations": {"type": int, "default": 50},
                "db_optimization_interval_iterations": {"type": int, "default": 1000},
            },
        },
        "pueue": {
            "required": True,
            "fields": {
                "groups": {"type": list, "required": True},
            },
        },
        "tps_generator": {
            "required": False,
            "fields": {
                "validation": {
                    "type": dict, 
                    "required": False,
                    "default": {
                        "required_params": ["GPUID", "DicomDir", "logFilePath", "OutputDir", "BeamNumbers"]
                    }
                },
                "default_paths": {
                    "type": dict,
                    "required": False,
                    "default": {
                        "base_dir": "/home/gpuadmin/MOQUI_SMC",
                        "interpreter_outputs_dir": "/home/gpuadmin/Outputs_csv",
                        "outputs_dir": "/home/gpuadmin/Dose_raw"
                    }
                }
            },
        },
        "moqui_tps_parameters": {
            "required": False,
            "fields": {
                "GPUID": {"type": int, "default": 0},
                "RandomSeed": {"type": int, "default": -1932780356},
                "UseAbsolutePath": {"type": bool, "default": True},
                "Verbosity": {"type": int, "default": 0},
                "UsingPhantomGeo": {"type": bool, "default": True},
                "PhantomDimX": {"type": int, "default": 400},
                "PhantomDimY": {"type": int, "default": 400},
                "PhantomDimZ": {"type": int, "default": 400},
                "PhantomUnitX": {"type": int, "default": 1},
                "PhantomUnitY": {"type": int, "default": 1},
                "PhantomUnitZ": {"type": int, "default": 1},
                "PhantomPositionX": {"type": float, "default": -200.0},
                "PhantomPositionY": {"type": float, "default": -200.0},
                "PhantomPositionZ": {"type": float, "default": -380.0},
                "Scorer": {"type": str, "default": "Dose"},
                "SupressStd": {"type": bool, "default": True},
                "ReadStructure": {"type": bool, "default": True},
                "ROIName": {"type": str, "default": "External"},
                "DicomDir": {"type": str, "default": ""},
                "logFilePath": {"type": str, "default": ""},
                "SourceType": {"type": str, "default": "FluenceMap"},
                "SimulationType": {"type": str, "default": "perBeam"},
                "ScoreToCTGrid": {"type": bool, "default": True},
                "OutputFormat": {"type": str, "default": "raw"},
                "OverwriteResults": {"type": bool, "default": True},
                "TotalThreads": {"type": int, "default": -1},
                "MaxHistoriesPerBatch": {"type": int, "default": 10000},
                "BeamNumbers": {"type": int, "default": 1},
                "ParticlesPerHistory": {"type": int, "default": 1},
                "TwoCentimeterMode": {"type": bool, "default": True},
                "ParentDir": {"type": str, "default": ""},
                "OutputDir": {"type": str, "default": ""},
                "GantryNum": {"type": int, "default": 0},
            },
        },
        "local_tools": {
            "required": False,
            "fields": {
                "mqi_interpreter": {"type": str, "required": False},
                "raw2dcm": {"type": str, "required": False},
            },
        },
        "main_workflow": {
            "required": False,
            "fields": {},  # This is a complex list structure - validation handled separately
        },
        "post_processing": {
            "required": False,
            "fields": {
                "download_results": {
                    "type": dict,
                    "required": False,
                    "default": {
                        "enabled": True,
                        "remote_filename": "RTDOSE.dcm",
                        "local_destination_dir": "completed_cases"
                    }
                }
            },
        },
    }

    def __init__(self, config_path: str):
        """
        Initialize ConfigManager with configuration file.

        Args:
            config_path: Path to the YAML configuration file

        Raises:
            ConfigValidationError: If configuration is invalid or missing
        """
        self.config_path = config_path
        self.config = self._load_and_validate_config()

    def _load_and_validate_config(self) -> Dict[str, Any]:
        """Load and validate configuration from file."""
        # Check if config file exists
        if not Path(self.config_path).exists():
            raise ConfigValidationError(f"Config file not found: {self.config_path}")

        # Load YAML
        try:
            with open(self.config_path, "r") as f:
                config = yaml.safe_load(f)
        except yaml.YAMLError as e:
            raise ConfigValidationError(
                f"Invalid YAML format in {self.config_path}: {e}"
            )

        if not isinstance(config, dict):
            raise ConfigValidationError("Configuration must be a YAML dictionary")

        # Apply defaults and validate
        validated_config = self._apply_defaults_and_validate(config)
        return validated_config

    def _apply_defaults_and_validate(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """Apply default values and validate configuration against schema."""
        validated_config = {}

        for section_name, section_schema in self.SCHEMA.items():
            # Check required sections
            if section_schema.get("required", False) and section_name not in config:
                raise ConfigValidationError(f"Missing required section: {section_name}")

            # Get or create section
            section_config = config.get(section_name, {})

            # Special handling for main_workflow which is a list structure
            if section_name == "main_workflow":
                if section_name in config:
                    if not isinstance(config[section_name], list):
                        raise ConfigValidationError(
                            f"Invalid type for {section_name}: expected list, "
                            f"got {type(config[section_name]).__name__}"
                        )
                    validated_config[section_name] = config[section_name]
                continue

            validated_section = {}

            # Process fields in this section
            for field_name, field_schema in section_schema["fields"].items():
                field_key = f"{section_name}.{field_name}"

                # Check if field exists
                if field_name in section_config:
                    field_value = section_config[field_name]
                    # Validate type
                    expected_type = field_schema["type"]
                    if not isinstance(field_value, expected_type):
                        raise ConfigValidationError(
                            f"Invalid type for {field_key}: expected {expected_type.__name__}, "
                            f"got {type(field_value).__name__}"
                        )
                    validated_section[field_name] = field_value
                elif field_schema.get("required", False):
                    raise ConfigValidationError(f"Missing required field: {field_key}")
                elif "default" in field_schema:
                    validated_section[field_name] = field_schema["default"]

            if validated_section:  # Only add section if it has content
                validated_config[section_name] = validated_section

        return validated_config

    def get(self, key: str, default: Any = None) -> Any:
        """
        Get configuration value using dot notation.

        Args:
            key: Configuration key in dot notation (e.g., 'hpc.host')
            default: Default value to return if key not found

        Returns:
            Configuration value

        Raises:
            ConfigValidationError: If key not found and no default provided
        """
        parts = key.split(".")
        current = self.config

        for part in parts:
            if isinstance(current, dict) and part in current:
                current = current[part]
            else:
                if default is not None:
                    return default
                raise ConfigValidationError(f"Configuration key not found: {key}")

        return current

    def get_section(self, section_name: str) -> Dict[str, Any]:
        """
        Get entire configuration section.

        Args:
            section_name: Name of the configuration section

        Returns:
            Dictionary containing the section configuration

        Raises:
            ConfigValidationError: If section not found
        """
        if section_name not in self.config:
            raise ConfigValidationError(
                f"Configuration section not found: {section_name}"
            )

        return self.config[section_name].copy()

    def reload(self) -> None:
        """Reload configuration from file."""
        self.config = self._load_and_validate_config()
