"""
Tests for the DynamicGpuManager service.

This module tests dynamic GPU resource detection and management for HPC systems.
The DynamicGpuManager auto-detects available GPU resources from the remote HPC
and maintains them in the database.
"""

import pytest
from unittest.mock import Mock, patch
from src.services.dynamic_gpu_manager import DynamicGpuManager, GpuDetectionError
from src.common.db_manager import DatabaseManager
import json


class TestDynamicGpuManager:
    """Tests for the DynamicGpuManager class."""

    def test_initialization_with_valid_config(self):
        """Test that DynamicGpuManager initializes properly with valid config."""
        config = {
            "hpc": {
                "host": "test.hpc.com",
                "user": "testuser",
                "ssh_command": "ssh",
                "pueue_command": "pueue",
            }
        }
        db_manager = Mock(spec=DatabaseManager)

        gpu_manager = DynamicGpuManager(config, db_manager)

        assert gpu_manager.hpc_config == config["hpc"]
        assert gpu_manager.db_manager == db_manager
        assert gpu_manager.host == "test.hpc.com"
        assert gpu_manager.user == "testuser"

    def test_detect_available_gpu_groups_success(self):
        """Test successful detection of available GPU groups from Pueue."""
        config = {
            "hpc": {
                "host": "test.hpc.com",
                "user": "testuser",
                "ssh_command": "ssh",
                "pueue_command": "pueue",
            }
        }
        db_manager = Mock(spec=DatabaseManager)
        gpu_manager = DynamicGpuManager(config, db_manager)

        # Mock pueue group output
        mock_groups_output = """Groups
======
Group "default" (0 parallel): Running
Group "gpu_a" (1 parallel): Running
Group "gpu_b" (1 parallel): Running  
Group "gpu_c" (2 parallel): Running"""

        with patch("subprocess.run") as mock_run:
            mock_run.return_value.stdout = mock_groups_output
            mock_run.return_value.stderr = ""
            mock_run.return_value.returncode = 0

            groups = gpu_manager.detect_available_gpu_groups()

            expected_groups = ["default", "gpu_a", "gpu_b", "gpu_c"]
            assert groups == expected_groups

    def test_detect_gpu_groups_ssh_failure(self):
        """Test handling of SSH connection failure during GPU detection."""
        config = {
            "hpc": {
                "host": "unreachable.hpc.com",
                "user": "testuser",
                "ssh_command": "ssh",
                "pueue_command": "pueue",
            }
        }
        db_manager = Mock(spec=DatabaseManager)
        gpu_manager = DynamicGpuManager(config, db_manager)

        with patch("subprocess.run") as mock_run:
            mock_run.side_effect = Exception("Connection refused")

            with pytest.raises(GpuDetectionError) as exc_info:
                gpu_manager.detect_available_gpu_groups()

            assert "Failed to detect GPU groups" in str(exc_info.value)
            assert "Connection refused" in str(exc_info.value)

    def test_get_gpu_resource_utilization_success(self):
        """Test successful retrieval of GPU resource utilization."""
        config = {
            "hpc": {
                "host": "test.hpc.com",
                "user": "testuser",
                "ssh_command": "ssh",
                "pueue_command": "pueue",
            }
        }
        db_manager = Mock(spec=DatabaseManager)
        gpu_manager = DynamicGpuManager(config, db_manager)

        # Mock pueue status JSON output
        mock_status = {
            "groups": {
                "default": {"running": 0, "queued": 0},
                "gpu_a": {"running": 1, "queued": 2},
                "gpu_b": {"running": 0, "queued": 0},
            }
        }

        with patch("subprocess.run") as mock_run:
            mock_run.return_value.stdout = json.dumps(mock_status)
            mock_run.return_value.stderr = ""
            mock_run.return_value.returncode = 0

            utilization = gpu_manager.get_gpu_resource_utilization()

            expected_utilization = {
                "default": {"running": 0, "queued": 0, "total_load": 0},
                "gpu_a": {"running": 1, "queued": 2, "total_load": 3},
                "gpu_b": {"running": 0, "queued": 0, "total_load": 0},
            }
            assert utilization == expected_utilization

    def test_sync_gpu_resources_with_database(self):
        """Test synchronization of detected GPU resources with database."""
        config = {
            "hpc": {
                "host": "test.hpc.com",
                "user": "testuser",
                "ssh_command": "ssh",
                "pueue_command": "pueue",
            }
        }
        db_manager = Mock(spec=DatabaseManager)
        gpu_manager = DynamicGpuManager(config, db_manager)

        # Mock detected groups
        with patch.object(gpu_manager, "detect_available_gpu_groups") as mock_detect:
            mock_detect.return_value = ["default", "gpu_a", "gpu_b", "gpu_new"]

            # Mock database methods
            db_manager.get_resources_by_status.return_value = [
                {"pueue_group": "default", "status": "available"},
                {
                    "pueue_group": "gpu_old",
                    "status": "available",
                },  # This should be removed
            ]

            gpu_manager.sync_gpu_resources_with_database()

            # Should ensure new resources exist
            db_manager.ensure_gpu_resource_exists.assert_any_call("gpu_a")
            db_manager.ensure_gpu_resource_exists.assert_any_call("gpu_b")
            db_manager.ensure_gpu_resource_exists.assert_any_call("gpu_new")

    def test_get_optimal_gpu_assignment(self):
        """Test optimal GPU assignment based on current utilization."""
        config = {
            "hpc": {
                "host": "test.hpc.com",
                "user": "testuser",
                "ssh_command": "ssh",
                "pueue_command": "pueue",
            }
        }
        db_manager = Mock(spec=DatabaseManager)
        gpu_manager = DynamicGpuManager(config, db_manager)

        # Mock utilization data
        with patch.object(gpu_manager, "get_gpu_resource_utilization") as mock_util:
            mock_util.return_value = {
                "gpu_a": {"running": 2, "queued": 1, "total_load": 3},
                "gpu_b": {"running": 0, "queued": 0, "total_load": 0},
                "gpu_c": {"running": 1, "queued": 0, "total_load": 1},
            }

            # Mock database availability
            db_manager.get_gpu_resource.side_effect = lambda group: {
                "pueue_group": group,
                "status": "available" if group in ["gpu_b", "gpu_c"] else "assigned",
            }

            # Mock hardware utilization and mapping
            with patch.object(gpu_manager, "get_gpu_hardware_utilization") as mock_hw, \
                 patch.object(gpu_manager, "map_gpu_groups_to_indices") as mock_mapping:
                
                mock_hw.return_value = {
                    1: {"utilization_gpu": 0, "memory_percent": 2.0, "is_busy": False},
                    2: {"utilization_gpu": 0, "memory_percent": 1.0, "is_busy": False}
                }
                mock_mapping.return_value = {
                    "gpu_a": [0],
                    "gpu_b": [1],
                    "gpu_c": [2]
                }

                optimal_group = gpu_manager.get_optimal_gpu_assignment()

                # Should return gpu_b (least loaded and available)
                assert optimal_group == "gpu_b"

    def test_refresh_gpu_resources_complete_workflow(self):
        """Test the complete workflow of refreshing GPU resources."""
        config = {
            "hpc": {
                "host": "test.hpc.com",
                "user": "testuser",
                "ssh_command": "ssh",
                "pueue_command": "pueue",
            }
        }
        db_manager = Mock(spec=DatabaseManager)
        gpu_manager = DynamicGpuManager(config, db_manager)

        # Mock all the sub-methods including new hardware utilization
        with patch.object(
            gpu_manager, "detect_available_gpu_groups"
        ) as mock_detect, patch.object(
            gpu_manager, "sync_gpu_resources_with_database"
        ) as mock_sync, patch.object(
            gpu_manager, "get_gpu_resource_utilization"
        ) as mock_util, patch.object(
            gpu_manager, "get_gpu_hardware_utilization"
        ) as mock_hw, patch.object(
            gpu_manager, "map_gpu_groups_to_indices"
        ) as mock_mapping:

            mock_detect.return_value = ["gpu_a", "gpu_b"]
            mock_util.return_value = {
                "gpu_a": {"running": 1, "queued": 0, "total_load": 1},
                "gpu_b": {"running": 0, "queued": 0, "total_load": 0},
            }
            mock_hw.return_value = {
                0: {"utilization_gpu": 15, "memory_percent": 25.0, "is_busy": True},
                1: {"utilization_gpu": 2, "memory_percent": 5.0, "is_busy": False}
            }
            mock_mapping.return_value = {
                "gpu_a": [0],
                "gpu_b": [1]
            }

            # Add this line to mock the get_all_gpu_resources method
            db_manager.get_all_gpu_resources.return_value = []

            result = gpu_manager.refresh_gpu_resources()

            mock_detect.assert_called_once()
            mock_sync.assert_called_once()
            mock_util.assert_called_once()
            mock_hw.assert_called_once()
            mock_mapping.assert_called_once()

            expected_result = {
                "detected_groups": ["gpu_a", "gpu_b"],
                "pueue_utilization": {
                    "gpu_a": {"running": 1, "queued": 0, "total_load": 1},
                    "gpu_b": {"running": 0, "queued": 0, "total_load": 0},
                },
                "hardware_utilization": {
                    0: {"utilization_gpu": 15, "memory_percent": 25.0, "is_busy": True},
                    1: {"utilization_gpu": 2, "memory_percent": 5.0, "is_busy": False}
                },
                "group_to_indices": {
                    "gpu_a": [0],
                    "gpu_b": [1]
                }
            }
            assert result == expected_result

    def test_get_gpu_hardware_utilization_success(self):
        """Test successful retrieval of GPU hardware utilization via nvidia-smi."""
        config = {
            "hpc": {
                "host": "test.hpc.com",
                "user": "testuser",
                "ssh_command": "ssh",
                "pueue_command": "pueue",
            },
            "curator": {
                "gpu_monitor_command": "nvidia-smi --query-gpu=index,uuid,utilization.gpu,memory.used,memory.total,temperature.gpu --format=csv,noheader,nounits"
            }
        }
        db_manager = Mock(spec=DatabaseManager)
        gpu_manager = DynamicGpuManager(config, db_manager)

        # Mock nvidia-smi output - GPU 0 idle, GPU 1 busy
        mock_nvidia_output = (
            "0,GPU-12345678,5,512,8192,45\n"
            "1,GPU-87654321,85,7168,8192,78\n"
            "2,GPU-11223344,0,12,8192,35"
        )

        with patch("subprocess.run") as mock_run:
            mock_result = Mock()
            mock_result.stdout = mock_nvidia_output
            mock_result.returncode = 0
            mock_run.return_value = mock_result

            utilization = gpu_manager.get_gpu_hardware_utilization()

            # Verify subprocess call
            mock_run.assert_called_once()
            call_args = mock_run.call_args[0][0]
            assert "ssh" in call_args
            assert "testuser@test.hpc.com" in call_args
            assert "nvidia-smi" in " ".join(call_args)

            # Verify parsed results
            expected = {
                0: {
                    "utilization_gpu": 5,
                    "memory_used": 512,
                    "memory_total": 8192,
                    "memory_percent": 6.25,
                    "is_busy": False  # 5% util, 6.25% mem - both under thresholds
                },
                1: {
                    "utilization_gpu": 85,
                    "memory_used": 7168,
                    "memory_total": 8192,
                    "memory_percent": 87.5,
                    "is_busy": True  # 85% util - over threshold
                },
                2: {
                    "utilization_gpu": 0,
                    "memory_used": 12,
                    "memory_total": 8192,
                    "memory_percent": 0.146484375,
                    "is_busy": False  # 0% util, minimal memory
                }
            }
            assert utilization == expected

    def test_map_gpu_groups_to_indices_success(self):
        """Test successful mapping of GPU groups to hardware indices."""
        config = {
            "hpc": {
                "host": "test.hpc.com",
                "user": "testuser",
                "ssh_command": "ssh",
                "pueue_command": "pueue",
            }
        }
        db_manager = Mock(spec=DatabaseManager)
        gpu_manager = DynamicGpuManager(config, db_manager)

        with patch.object(gpu_manager, "detect_available_gpu_groups") as mock_detect:
            mock_detect.return_value = ["gpu_0", "gpu_1", "gpu_7", "gpu_special"]

            mapping = gpu_manager.map_gpu_groups_to_indices()

            expected = {
                "gpu_0": [0],
                "gpu_1": [1], 
                "gpu_7": [7],
                "gpu_special": []  # No numeric suffix
            }
            assert mapping == expected

    def test_optimal_gpu_assignment_respects_hardware_utilization(self):
        """Test that optimal GPU assignment considers hardware utilization."""
        config = {
            "hpc": {
                "host": "test.hpc.com",
                "user": "testuser",
                "ssh_command": "ssh",
                "pueue_command": "pueue",
            }
        }
        db_manager = Mock(spec=DatabaseManager)
        gpu_manager = DynamicGpuManager(config, db_manager)

        # Mock Pueue utilization - all groups appear idle
        pueue_utilization = {
            "gpu_7": {"running": 0, "queued": 0, "total_load": 0},
            "gpu_8": {"running": 0, "queued": 0, "total_load": 0}
        }

        # Mock hardware utilization - GPU 7 is actually busy
        hardware_utilization = {
            7: {"utilization_gpu": 85, "memory_percent": 80.0, "is_busy": True},
            8: {"utilization_gpu": 2, "memory_percent": 1.5, "is_busy": False}
        }

        group_mapping = {
            "gpu_7": [7],
            "gpu_8": [8]
        }

        # Mock database - both GPUs available in DB
        db_manager.get_gpu_resource.side_effect = lambda name: {"status": "available"}

        with patch.object(gpu_manager, "get_gpu_resource_utilization") as mock_pueue, \
             patch.object(gpu_manager, "get_gpu_hardware_utilization") as mock_hw, \
             patch.object(gpu_manager, "map_gpu_groups_to_indices") as mock_mapping:

            mock_pueue.return_value = pueue_utilization
            mock_hw.return_value = hardware_utilization
            mock_mapping.return_value = group_mapping

            optimal = gpu_manager.get_optimal_gpu_assignment()

            # Should select gpu_8 (hardware idle) not gpu_7 (hardware busy)
            assert optimal == "gpu_8"
