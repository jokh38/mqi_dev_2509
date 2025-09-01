"""
TPS generator module for creating moqui_tps.in configuration files.

This module is responsible for dynamically generating moqui_tps.in files
based on case data, DICOM information, and configuration parameters.
"""

import re
import pathlib
from pathlib import Path
from typing import Dict, Any, Optional, List
from copy import deepcopy

from src.common.structured_logging import get_structured_logger, LogContext

logger = get_structured_logger(__name__)


class TpsGeneratorError(Exception):
    """Custom exception for TPS generation errors."""
    pass


def extract_gpu_id_from_group(pueue_group: str) -> int:
    """
    Extract GPU ID from pueue group name.
    
    Args:
        pueue_group: Pueue group name (e.g., 'gpu_0', 'gpu0', 'default')
        
    Returns:
        Integer GPU ID, defaults to 0 if extraction fails
    """
    match = re.search(r'gpu[_-]?(\d+)', pueue_group.lower())
    if match:
        return int(match.group(1))
    return 0


def create_ini_content(case_data: Dict[str, Any], base_params: Dict[str, Any], 
                      dicom_info: Optional[Dict[str, Any]] = None,
                      hpc_config: Optional[Dict[str, Any]] = None,
                      tps_generator_config: Optional[Dict[str, Any]] = None) -> str:
    """
    Create moqui_tps.in file content from case data and base parameters.
    
    Args:
        case_data: Dictionary containing case information including:
                  - case_id: Database ID of the case
                  - case_path: Local path to the case directory
                  - pueue_group: GPU group assignment
        base_params: Base moqui_tps_parameters from configuration
        dicom_info: Optional DICOM information from dicom_parser
        hpc_config: Optional HPC configuration for remote paths
        tps_generator_config: Optional TPS generator configuration
        
    Returns:
        String content for moqui_tps.in file in key-value format
        
    Raises:
        TpsGeneratorError: If required parameters are missing or invalid
    """
    if not case_data:
        raise TpsGeneratorError("case_data is required")
    if not base_params:
        raise TpsGeneratorError("base_params is required")
    
    # Create a copy of base parameters to avoid modifying original
    params = deepcopy(base_params)
    
    try:
        # Extract case information
        case_id = case_data.get('case_id')
        case_path = case_data.get('case_path', '')
        pueue_group = case_data.get('pueue_group', 'default')
        
        if not case_id:
            raise TpsGeneratorError("case_id is required in case_data")
        if not case_path:
            raise TpsGeneratorError("case_path is required in case_data")
            
        case_name = Path(case_path).name
        
        # Set dynamic GPU ID from pueue group
        params['GPUID'] = extract_gpu_id_from_group(pueue_group)
        
        # Get path configurations
        paths_config = {}
        if hpc_config and hpc_config.get('remote_base_dir'):
            paths_config['base_dir'] = hpc_config.get('remote_base_dir')
            paths_config['interpreter_outputs_dir'] = hpc_config.get('moqui_interpreter_outputs_dir')
            paths_config['outputs_dir'] = hpc_config.get('moqui_outputs_dir')
        elif tps_generator_config:
            paths_config = tps_generator_config.get("default_paths", {})

        # Set dynamic paths
        base_dir = paths_config.get('base_dir')
        interpreter_outputs_dir = paths_config.get('interpreter_outputs_dir')
        outputs_dir = paths_config.get('outputs_dir')

        if not all([base_dir, interpreter_outputs_dir, outputs_dir]):
            raise TpsGeneratorError("Path configuration is missing in config.yaml (hpc or tps_generator.default_paths)")

        # Use pathlib for robust path construction, ensuring forward slashes for the remote Linux system.
        case_name = Path(case_path).name
        params['DicomDir'] = str(pathlib.Path(base_dir) / case_name).replace('\\', '/')
        case_log_path = str(pathlib.Path(interpreter_outputs_dir) / case_name).replace('\\', '/')
        params['logFilePath'] = case_log_path
        params['ParentDir'] = case_log_path
        params['OutputDir'] = str(pathlib.Path(outputs_dir) / case_name).replace('\\', '/')
        
        # Set DICOM-derived parameters if available
        if dicom_info:
            beams = dicom_info.get('beams', [])
            
            # Count non-setup beams
            treatment_beams = [
                beam for beam in beams 
                if beam.get('beam_name', '').upper() != 'SETUP' and
                'SETUP' not in beam.get('beam_name', '').upper()
            ]
            
            if treatment_beams:
                params['BeamNumbers'] = len(treatment_beams)
                
                # If we have beam information, use the first treatment beam's gantry angle
                first_beam = treatment_beams[0]
                gantry_angle = first_beam.get('gantry_angle')
                if gantry_angle is not None:
                    params['GantryNum'] = int(gantry_angle)
            else:
                logger.warning(
                    "No treatment beams found in DICOM info, using default beam count",
                    context=LogContext(
                        case_id=str(case_id),
                        operation="tps_generation",
                        extra_data={"total_beams": len(beams)}
                    ).to_dict()
                )
        
        # Convert parameters to INI format
        ini_content_lines = []
        ini_content_lines.append("# Key-Value format. Values are populated dynamically at runtime.")
        ini_content_lines.append("")
        
        for key, value in params.items():
            # Convert boolean values to lowercase strings
            if isinstance(value, bool):
                value_str = str(value).lower()
            else:
                value_str = str(value)
            
            ini_content_lines.append(f"{key} {value_str}")
        
        logger.info(
            "Successfully generated moqui_tps.in content",
            context=LogContext(
                case_id=str(case_id),
                operation="tps_generation",
                extra_data={
                    "case_name": case_name,
                    "gpu_id": params['GPUID'],
                    "beam_count": params['BeamNumbers'],
                    "gantry_angle": params['GantryNum']
                }
            ).to_dict()
        )
        
        return "\n".join(ini_content_lines)
        
    except Exception as e:
        error_msg = f"Failed to generate TPS content: {str(e)}"
        logger.error_with_exception(
            "TPS generation failed",
            e,
            context=LogContext(
                case_id=str(case_data.get('case_id', 'unknown')),
                operation="tps_generation"
            ).to_dict()
        )
        raise TpsGeneratorError(error_msg) from e


def validate_ini_content(ini_content: str, required_params: List[str]) -> bool:
    """
    Validate generated INI content for required parameters.
    
    Args:
        ini_content: Generated INI file content as a string.
        required_params: A list of required parameter names.
        
    Returns:
        True if content is valid, False otherwise.
    """
    if not ini_content:
        return False
    
    # Create a set of parameters found in the content for efficient lookup.
    present_params = {line.split(' ')[0] for line in ini_content.strip().split('\n') if line}

    missing_params = [param for param in required_params if param not in present_params]
    
    if missing_params:
        logger.error(f"Missing required parameters in INI content: {', '.join(missing_params)}")
        return False
    
    return True