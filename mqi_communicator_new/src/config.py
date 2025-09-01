"""
Pydantic-based configuration loader and validator.
Provides automatic, fail-fast validation of the configuration structure.
"""
from typing import Dict, Any
from pydantic import BaseModel, ValidationError, Field
import yaml
from pathlib import Path


class ApplicationConfig(BaseModel):
    max_workers: int = Field(default=4, ge=1, le=16, description="Number of concurrent workers")
    scan_interval_seconds: int = Field(default=60, ge=10, description="Directory scan interval")
    polling_interval_seconds: int = Field(default=300, ge=60, description="HPC polling interval")

class ExecutablesConfig(BaseModel):
    python_interpreter: str = Field(description="Path to Python interpreter")
    mqi_interpreter: str = Field(description="Path to mqi_interpreter main_cli.py")
    raw_to_dicom: str = Field(description="Path to RawToDCM converter")

class LocalPathsConfig(BaseModel):
    scan_directory: str = Field(description="Directory to watch for new cases")
    database_path: str = Field(description="Path to the SQLite database file")
    processing_directory: str = Field(description="Template path with {case_id} placeholder")
    raw_output_directory: str = Field(description="Template path for raw outputs")
    final_dicom_directory: str = Field(description="Template path for DICOM outputs")

class HpcPathsConfig(BaseModel):
    base_dir: str = Field(description="HPC base directory")
    tps_env_dir: str = Field(description="TPS environment directory")
    output_csv_dir: str = Field(description="CSV output directory template")
    dose_raw_dir: str = Field(description="Raw dose output directory template")

class PathsConfig(BaseModel):
    local: LocalPathsConfig
    hpc: HpcPathsConfig

class HpcConnectionConfig(BaseModel):
    host: str = Field(description="HPC hostname")
    port: int = Field(default=22, ge=1, le=65535, description="SSH port")
    user: str = Field(description="SSH username")
    ssh_key_path: str = Field(description="Path to SSH private key")

class Config(BaseModel):
    """Root configuration model with all sections"""
    application: ApplicationConfig
    executables: ExecutablesConfig
    paths: PathsConfig
    hpc_connection: HpcConnectionConfig


class ConfigManager:
    """
    Manager for application configuration using Pydantic models.
    
    Responsibilities:
    1. Load and parse the config.yaml file
    2. Validate configuration structure and values using Pydantic
    3. Provide access to validated configuration data
    4. Exit with clear error messages on configuration issues
    """
    
    def __init__(self, config_path: str) -> None:
        """
        Initialize the ConfigManager with a path to the configuration file.
        
        Args:
            config_path: Path to the config.yaml file
        """
        self.config_path = Path(config_path)
        if not self.config_path.is_file():
            raise FileNotFoundError(f"Configuration file not found at: {self.config_path}")

        try:
            with open(self.config_path, 'r') as f:
                config_data = yaml.safe_load(f)
            self.config = Config.model_validate(config_data)
        except yaml.YAMLError as e:
            raise ValueError(f"Error parsing YAML file: {e}")
        except ValidationError as e:
            raise ValueError(f"Configuration validation error: {e}")

    def get_config(self) -> Config:
        """
        Get the validated configuration data.
        
        Returns:
            Pydantic model containing the validated configuration
        """
        return self.config
    
    def get_application_config(self) -> ApplicationConfig:
        """Get application configuration section"""
        return self.config.application

    def get_paths_config(self) -> PathsConfig:
        """Get paths configuration section"""
        return self.config.paths

    def get_hpc_connection_config(self) -> HpcConnectionConfig:
        """Get HPC connection configuration section"""
        return self.config.hpc_connection

    def get_executables_config(self) -> ExecutablesConfig:
        """Get executables configuration section"""
        return self.config.executables

    def resolve_case_path(self, template_path: str, case_id: str) -> str:
        """Resolve template path with case_id placeholder"""
        return template_path.format(case_id=case_id)