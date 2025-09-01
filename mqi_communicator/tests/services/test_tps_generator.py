"""
Tests for the TPS generator module.
"""

import pytest
from pathlib import Path
from unittest.mock import patch, MagicMock

from src.services.tps_generator import (
    extract_gpu_id_from_group,
    create_ini_content,
    validate_ini_content,
    TpsGeneratorError
)


class TestExtractGpuIdFromGroup:
    """Test GPU ID extraction from pueue group names."""
    
    def test_gpu_underscore_format(self):
        """Test extraction from 'gpu_0' format."""
        assert extract_gpu_id_from_group('gpu_0') == 0
        assert extract_gpu_id_from_group('gpu_3') == 3
        assert extract_gpu_id_from_group('gpu_7') == 7
    
    def test_gpu_no_separator_format(self):
        """Test extraction from 'gpu0' format."""
        assert extract_gpu_id_from_group('gpu0') == 0
        assert extract_gpu_id_from_group('gpu3') == 3
        assert extract_gpu_id_from_group('gpu7') == 7
    
    def test_gpu_hyphen_format(self):
        """Test extraction from 'gpu-0' format."""
        assert extract_gpu_id_from_group('gpu-0') == 0
        assert extract_gpu_id_from_group('gpu-3') == 3
    
    def test_case_insensitive(self):
        """Test case insensitive extraction."""
        assert extract_gpu_id_from_group('GPU_0') == 0
        assert extract_gpu_id_from_group('Gpu_3') == 3
        assert extract_gpu_id_from_group('GPU0') == 0
    
    def test_no_gpu_format(self):
        """Test fallback to 0 for non-GPU formats."""
        assert extract_gpu_id_from_group('default') == 0
        assert extract_gpu_id_from_group('queue1') == 0
        assert extract_gpu_id_from_group('invalid') == 0
        assert extract_gpu_id_from_group('') == 0


class TestCreateIniContent:
    """Test INI content creation."""
    
    def test_minimal_case_data(self):
        """Test INI creation with minimal case data."""
        case_data = {
            'case_id': 123,
            'case_path': '/path/to/case',
            'pueue_group': 'gpu_2'
        }
        
        base_params = {
            'GPUID': 0,
            'RandomSeed': -1932780356,
            'UseAbsolutePath': True,
            'Verbosity': 0,
            'BeamNumbers': 1,
            'DicomDir': '',
            'logFilePath': '',
            'OutputDir': '',
            'ParentDir': '',
            'GantryNum': 0
        }
        
        tps_generator_config = {
            'default_paths': {
                'base_dir': '/home/gpuadmin/MOQUI_SMC',
                'interpreter_outputs_dir': '/home/gpuadmin/Outputs_csv',
                'outputs_dir': '/home/gpuadmin/Dose_raw'
            }
        }
        
        result = create_ini_content(case_data, base_params, tps_generator_config=tps_generator_config)
        
        # Check that GPU ID was extracted correctly
        assert 'GPUID 2' in result
        assert 'RandomSeed -1932780356' in result
        assert 'UseAbsolutePath true' in result
        assert 'Verbosity 0' in result
        assert 'BeamNumbers 1' in result
        
        # Check header comment
        assert '# Key-Value format' in result
    
    def test_with_hpc_config(self):
        """Test INI creation with HPC configuration."""
        case_data = {
            'case_id': 123,
            'case_path': '/local/path/case_name',
            'pueue_group': 'gpu_1'
        }
        
        base_params = {
            'GPUID': 0,
            'DicomDir': '',
            'logFilePath': '',
            'OutputDir': '',
            'ParentDir': '',
            'BeamNumbers': 1,
            'GantryNum': 0
        }
        
        hpc_config = {
            'remote_base_dir': '~/MOQUI_SMC',
            'moqui_interpreter_outputs_dir': '~/Outputs_csv',
            'moqui_outputs_dir': '~/Dose_raw'
        }
        
        result = create_ini_content(case_data, base_params, hpc_config=hpc_config)
        
        # Check dynamic paths
        assert 'DicomDir ~/MOQUI_SMC/case_name' in result
        assert 'logFilePath ~/Outputs_csv/case_name' in result
        assert 'ParentDir ~/Outputs_csv/case_name' in result
        assert 'OutputDir ~/Dose_raw/case_name' in result
        assert 'GPUID 1' in result
    
    def test_with_dicom_info(self):
        """Test INI creation with DICOM information."""
        case_data = {
            'case_id': 123,
            'case_path': '/path/to/case',
            'pueue_group': 'gpu_0'
        }
        
        base_params = {
            'GPUID': 0,
            'BeamNumbers': 1,
            'GantryNum': 0,
            'DicomDir': '',
            'logFilePath': '',
            'OutputDir': '',
            'ParentDir': ''
        }
        
        dicom_info = {
            'beams': [
                {'beam_name': 'Beam1', 'gantry_angle': 45.0},
                {'beam_name': 'Beam2', 'gantry_angle': 90.0},
                {'beam_name': 'SETUP', 'gantry_angle': 0.0}  # Should be filtered out
            ]
        }
        
        tps_generator_config = {
            'default_paths': {
                'base_dir': '/home/gpuadmin/MOQUI_SMC',
                'interpreter_outputs_dir': '/home/gpuadmin/Outputs_csv',
                'outputs_dir': '/home/gpuadmin/Dose_raw'
            }
        }
        
        result = create_ini_content(case_data, base_params, dicom_info=dicom_info, tps_generator_config=tps_generator_config)
        
        # Should count only non-setup beams
        assert 'BeamNumbers 2' in result
        # Should use first treatment beam's gantry angle
        assert 'GantryNum 45' in result
    
    def test_boolean_values_formatting(self):
        """Test that boolean values are formatted correctly."""
        case_data = {
            'case_id': 123,
            'case_path': '/path/to/case',
            'pueue_group': 'gpu_0'
        }
        
        base_params = {
            'UseAbsolutePath': True,
            'SupressStd': False,
            'ReadStructure': True,
            'GPUID': 0,
            'DicomDir': '',
            'logFilePath': '',
            'OutputDir': '',
            'ParentDir': '',
            'BeamNumbers': 1,
            'GantryNum': 0
        }
        
        tps_generator_config = {
            'default_paths': {
                'base_dir': '/home/gpuadmin/MOQUI_SMC',
                'interpreter_outputs_dir': '/home/gpuadmin/Outputs_csv',
                'outputs_dir': '/home/gpuadmin/Dose_raw'
            }
        }
        
        result = create_ini_content(case_data, base_params, tps_generator_config=tps_generator_config)
        
        assert 'UseAbsolutePath true' in result
        assert 'SupressStd false' in result
        assert 'ReadStructure true' in result
    
    def test_missing_case_id(self):
        """Test error handling for missing case_id."""
        case_data = {
            'case_path': '/path/to/case',
            'pueue_group': 'gpu_0'
        }
        
        base_params = {'GPUID': 0}
        
        with pytest.raises(TpsGeneratorError, match="case_id is required"):
            create_ini_content(case_data, base_params)
    
    def test_missing_case_path(self):
        """Test error handling for missing case_path."""
        case_data = {
            'case_id': 123,
            'pueue_group': 'gpu_0'
        }
        
        base_params = {'GPUID': 0}
        
        with pytest.raises(TpsGeneratorError, match="case_path is required"):
            create_ini_content(case_data, base_params)
    
    def test_empty_case_data(self):
        """Test error handling for empty case_data."""
        with pytest.raises(TpsGeneratorError, match="case_data is required"):
            create_ini_content(None, {'GPUID': 0})
    
    def test_empty_base_params(self):
        """Test error handling for empty base_params."""
        case_data = {
            'case_id': 123,
            'case_path': '/path/to/case',
            'pueue_group': 'gpu_0'
        }
        
        with pytest.raises(TpsGeneratorError, match="base_params is required"):
            create_ini_content(case_data, None)
    
    def test_dicom_info_with_no_treatment_beams(self):
        """Test handling of DICOM info with only setup beams."""
        case_data = {
            'case_id': 123,
            'case_path': '/path/to/case',
            'pueue_group': 'gpu_0'
        }
        
        base_params = {
            'GPUID': 0,
            'BeamNumbers': 1,
            'GantryNum': 0,
            'DicomDir': '',
            'logFilePath': '',
            'OutputDir': '',
            'ParentDir': ''
        }
        
        dicom_info = {
            'beams': [
                {'beam_name': 'SETUP', 'gantry_angle': 0.0},
                {'beam_name': 'Site Setup', 'gantry_angle': 0.0}
            ]
        }
        
        tps_generator_config = {
            'default_paths': {
                'base_dir': '/home/gpuadmin/MOQUI_SMC',
                'interpreter_outputs_dir': '/home/gpuadmin/Outputs_csv',
                'outputs_dir': '/home/gpuadmin/Dose_raw'
            }
        }
        
        with patch('src.services.tps_generator.logger') as mock_logger:
            result = create_ini_content(case_data, base_params, dicom_info=dicom_info, tps_generator_config=tps_generator_config)
            
            # Should use default values since no treatment beams
            assert 'BeamNumbers 1' in result
            assert 'GantryNum 0' in result
            
            # Should log warning
            mock_logger.warning.assert_called_once()


class TestValidateIniContent:
    """Test INI content validation."""
    
    def test_valid_content(self):
        """Test validation of valid INI content."""
        valid_content = """
# Key-Value format
GPUID 0
DicomDir /path/to/dicom
logFilePath /path/to/logs
OutputDir /path/to/output
BeamNumbers 2
"""
        required = ['GPUID', 'DicomDir', 'logFilePath', 'OutputDir', 'BeamNumbers']
        assert validate_ini_content(valid_content, required) is True
    
    def test_missing_required_parameter(self):
        """Test validation failure for missing required parameter."""
        invalid_content = """
# Key-Value format
GPUID 0
DicomDir /path/to/dicom
logFilePath /path/to/logs
# Missing OutputDir and BeamNumbers
"""
        required = ['GPUID', 'DicomDir', 'logFilePath', 'OutputDir', 'BeamNumbers']
        with patch('src.services.tps_generator.logger') as mock_logger:
            assert validate_ini_content(invalid_content, required) is False
            # Should log error for missing parameters
            assert mock_logger.error.call_count >= 1
    
    def test_empty_content(self):
        """Test validation failure for empty content."""
        required = ['GPUID']
        assert validate_ini_content("", required) is False
        assert validate_ini_content(None, required) is False


if __name__ == '__main__':
    pytest.main([__file__])