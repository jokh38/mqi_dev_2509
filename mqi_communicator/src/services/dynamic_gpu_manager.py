"""
Dynamic GPU Resource Manager for HPC Systems.

This module provides dynamic detection and management of GPU resources
on remote HPC systems using Pueue queue management. It auto-detects
available GPU groups and maintains optimal resource allocation.
"""

import json
import logging
import subprocess
from typing import Dict, List, Optional, Any, Tuple

from src.common.db_manager import DatabaseManager
from src.common.structured_logging import get_structured_logger, LogContext

logger = get_structured_logger(__name__)


class GpuDetectionError(Exception):
    """Custom exception for GPU resource detection failures."""

    pass


class DynamicGpuManager:
    """
    Manages dynamic detection and allocation of GPU resources on HPC systems.

    This class interfaces with the remote Pueue daemon to detect available
    GPU groups, monitor their utilization, and provide optimal resource
    assignment for new computational cases.
    """

    def __init__(self, config: Dict[str, Any], db_manager: DatabaseManager) -> None:
        """
        Initialize the DynamicGpuManager.

        Args:
            config: Application configuration dictionary containing HPC settings
            db_manager: DatabaseManager instance for resource persistence
        """
        self.hpc_config = config["hpc"]
        self.db_manager = db_manager
        self.host = self.hpc_config["host"]
        self.user = self.hpc_config["user"]
        self.ssh_cmd = self.hpc_config.get("ssh_command", "ssh")
        self.pueue_cmd = self.hpc_config.get("pueue_command", "~/.cargo/bin/pueue")
        
        # Get GPU monitoring configuration
        curator_config = config.get("curator", {})
        self.gpu_monitor_cmd = curator_config.get(
            "gpu_monitor_command", 
            "nvidia-smi --query-gpu=index,uuid,utilization.gpu,memory.used,memory.total,temperature.gpu --format=csv,noheader,nounits"
        )

    def detect_available_gpu_groups(self) -> List[str]:
        """
        Detect available GPU groups from the remote Pueue daemon.

        Connects to the remote HPC system and queries Pueue for all
        available groups that can be used for job submission.

        Returns:
            List of available GPU group names

        Raises:
            GpuDetectionError: If detection fails due to connection or parsing issues
        """
        ssh_command = [
            self.ssh_cmd,
            f"{self.user}@{self.host}",
            self.pueue_cmd,
            "group",
        ]

        try:
            logger.info("Starting GPU group detection", LogContext(
                operation="detect_gpu_groups",
                extra_data={"host": self.host}
            ))
            result = subprocess.run(
                ssh_command, check=True, capture_output=True, text=True, timeout=30
            )

            available_groups = []  # Initialize the variable
            lines = result.stdout.splitlines()
            # Parse output format: Group "name" (N parallel): status
            for line in lines:
                if "Groups" in line or "======" in line or line.strip() == "":
                    continue
                # Extract group name from quotes
                if 'Group "' in line and '"' in line:
                    # Find the group name within quotes
                    start_quote = line.find('Group "') + 7  # Skip 'Group "'
                    end_quote = line.find('"', start_quote)
                    if end_quote != -1:
                        group_name = line[start_quote:end_quote]
                        available_groups.append(group_name)

            logger.info("Successfully detected GPU groups from HPC", LogContext(
                operation="detect_gpu_groups",
                extra_data={
                    "host": self.host,
                    "detected_count": len(available_groups),
                    "groups": available_groups
                }
            ))
            return available_groups

        except subprocess.CalledProcessError as e:
            error_msg = (
                f"Failed to detect GPU groups from {self.host}. "
                f"Command failed with exit code {e.returncode}.\n"
                f"Stderr: {e.stderr.strip()}"
            )
            logger.error_with_exception("GPU group detection failed with subprocess error", e, LogContext(
                operation="detect_gpu_groups",
                extra_data={
                    "host": self.host,
                    "exit_code": e.returncode,
                    "stderr": e.stderr.strip() if e.stderr else None
                }
            ))
            raise GpuDetectionError(error_msg) from e
        except Exception as e:
            error_msg = f"Failed to detect GPU groups from {self.host}: {str(e)}"
            logger.error_with_exception("GPU group detection failed with unexpected error", e, LogContext(
                operation="detect_gpu_groups",
                extra_data={"host": self.host}
            ))
            raise GpuDetectionError(error_msg) from e

    def get_gpu_resource_utilization(self) -> Dict[str, Dict[str, int]]:
        """
        Get current utilization of all GPU resources.

        Queries the remote Pueue daemon to get real-time utilization
        statistics for all available GPU groups.

        Returns:
            Dictionary mapping group names to utilization stats:
            {
                "gpu_a": {"running": 2, "queued": 1, "total_load": 3},
                "gpu_b": {"running": 0, "queued": 0, "total_load": 0}
            }

        Raises:
            GpuDetectionError: If utilization data cannot be retrieved
        """
        ssh_command = [
            self.ssh_cmd,
            f"{self.user}@{self.host}",
            self.pueue_cmd,
            "status",
            "--json",
        ]

        try:
            logger.debug("Querying GPU resource utilization from Pueue", LogContext(
                operation="get_utilization",
                extra_data={"host": self.host}
            ))
            result = subprocess.run(
                ssh_command, check=True, capture_output=True, text=True, timeout=30
            )

            status_data = json.loads(result.stdout)
            utilization = {}

            groups = status_data.get("groups", {})
            for group_name, group_data in groups.items():
                running = group_data.get("running", 0)
                queued = group_data.get("queued", 0)
                total_load = running + queued

                utilization[group_name] = {
                    "running": running,
                    "queued": queued,
                    "total_load": total_load,
                }

            return utilization

        except subprocess.CalledProcessError as e:
            error_msg = (
                f"Failed to get GPU utilization from {self.host}. "
                f"Command failed with exit code {e.returncode}.\n"
                f"Stderr: {e.stderr.strip()}"
            )
            logger.error_with_exception("Failed to get GPU utilization from Pueue", e, LogContext(
                operation="get_utilization",
                extra_data={
                    "host": self.host,
                    "exit_code": e.returncode,
                    "stderr": e.stderr.strip() if e.stderr else None
                }
            ))
            raise GpuDetectionError(error_msg) from e
        except Exception as e:
            error_msg = f"Failed to get GPU utilization from {self.host}: {str(e)}"
            logger.error_with_exception("Failed to get GPU utilization with unexpected error", e, LogContext(
                operation="get_utilization",
                extra_data={"host": self.host}
            ))
            raise GpuDetectionError(error_msg) from e

    def get_gpu_hardware_utilization(self) -> Dict[int, Dict[str, Any]]:
        """
        Get actual GPU hardware utilization using nvidia-smi.
        
        This method checks the actual GPU hardware usage, including
        processes running outside of Pueue queue management.
        
        Returns:
            Dictionary mapping GPU index to hardware utilization stats:
            {
                0: {"utilization_gpu": 85, "memory_used": 1024, "memory_total": 8192, "is_busy": True},
                1: {"utilization_gpu": 0, "memory_used": 12, "memory_total": 8192, "is_busy": False}
            }
        
        Raises:
            GpuDetectionError: If nvidia-smi command fails or output cannot be parsed
        """
        ssh_command = [
            self.ssh_cmd,
            f"{self.user}@{self.host}",
            self.gpu_monitor_cmd,
        ]

        try:
            logger.debug("Querying GPU hardware utilization via nvidia-smi", LogContext(
                operation="get_hardware_utilization",
                extra_data={"host": self.host}
            ))
            result = subprocess.run(
                ssh_command, check=True, capture_output=True, text=True, timeout=30
            )

            gpu_stats = {}
            lines = result.stdout.strip().split('\n')
            
            for line in lines:
                if line.strip():
                    # Parse CSV: index,uuid,utilization.gpu,memory.used,memory.total,temperature.gpu
                    parts = line.split(',')
                    if len(parts) >= 5:
                        try:
                            gpu_index = int(parts[0])
                            utilization_gpu = int(parts[2]) if parts[2].strip() else 0
                            memory_used = int(parts[3]) if parts[3].strip() else 0
                            memory_total = int(parts[4]) if parts[4].strip() else 0
                            
                            # Consider GPU busy if utilization > 5% OR memory usage > 10%
                            memory_percent = (memory_used / memory_total * 100) if memory_total > 0 else 0
                            is_busy = utilization_gpu > 5 or memory_percent > 10
                            
                            gpu_stats[gpu_index] = {
                                "utilization_gpu": utilization_gpu,
                                "memory_used": memory_used,
                                "memory_total": memory_total,
                                "memory_percent": memory_percent,
                                "is_busy": is_busy
                            }
                            
                            logger.debug("GPU hardware status parsed", LogContext(
                                operation="get_hardware_utilization",
                                extra_data={
                                    "gpu_index": gpu_index,
                                    "utilization_percent": utilization_gpu,
                                    "memory_percent": round(memory_percent, 1),
                                    "is_busy": is_busy
                                }
                            ))
                            
                        except (ValueError, IndexError) as parse_error:
                            logger.warning_with_exception("Failed to parse GPU stats line", parse_error, LogContext(
                                operation="get_hardware_utilization",
                                extra_data={"raw_line": line}
                            ))
                            continue

            logger.info("Successfully retrieved GPU hardware statistics", LogContext(
                operation="get_hardware_utilization",
                extra_data={
                    "host": self.host,
                    "gpu_count": len(gpu_stats)
                }
            ))
            return gpu_stats

        except subprocess.CalledProcessError as e:
            error_msg = (
                f"Failed to get GPU hardware utilization from {self.host}. "
                f"Command failed with exit code {e.returncode}.\n"
                f"Stderr: {e.stderr.strip()}"
            )
            logger.error_with_exception("Failed to get GPU hardware utilization", e, LogContext(
                operation="get_hardware_utilization",
                extra_data={
                    "host": self.host,
                    "exit_code": e.returncode,
                    "stderr": e.stderr.strip() if e.stderr else None
                }
            ))
            raise GpuDetectionError(error_msg) from e
        except Exception as e:
            error_msg = f"Failed to get GPU hardware utilization from {self.host}: {str(e)}"
            logger.error_with_exception("Failed to get GPU hardware utilization with unexpected error", e, LogContext(
                operation="get_hardware_utilization",
                extra_data={"host": self.host}
            ))
            raise GpuDetectionError(error_msg) from e

    def sync_gpu_resources_with_database(self) -> None:
        """
        Synchronize detected GPU resources with the local database.

        Ensures that all currently available GPU groups are represented
        in the local database as manageable resources.
        """
        try:
            # Get currently detected groups
            detected_groups = self.detect_available_gpu_groups()

            # Ensure all detected groups exist in database
            for group in detected_groups:
                self.db_manager.ensure_gpu_resource_exists(group)
                logger.debug("Ensured GPU resource exists in database", LogContext(
                    operation="sync_resources",
                    gpu_group=group
                ))

            logger.info("Successfully synchronized GPU resources with database", LogContext(
                operation="sync_resources",
                extra_data={
                    "synchronized_count": len(detected_groups),
                    "groups": detected_groups
                }
            ))

        except GpuDetectionError:
            # Re-raise detection errors
            raise
        except Exception as e:
            error_msg = f"Failed to sync GPU resources with database: {str(e)}"
            logger.error_with_exception("Failed to sync GPU resources with database", e, LogContext(
                operation="sync_resources"
            ))
            raise GpuDetectionError(error_msg) from e

    def map_gpu_groups_to_indices(self) -> Dict[str, List[int]]:
        """
        Map GPU group names to their corresponding hardware indices.
        
        This assumes group naming convention like 'gpu_0', 'gpu_1', etc.
        For more complex mappings, this method can be extended.
        
        Returns:
            Dictionary mapping group names to list of GPU indices:
            {"gpu_0": [0], "gpu_1": [1], "gpu_a": [2, 3]}
        """
        try:
            detected_groups = self.detect_available_gpu_groups()
            group_to_indices = {}
            
            for group_name in detected_groups:
                # Extract numeric indices from group names like 'gpu_0', 'gpu_1'
                if group_name.startswith('gpu_') and '_' in group_name:
                    try:
                        # Extract number after last underscore
                        index_str = group_name.split('_')[-1]
                        gpu_index = int(index_str)
                        group_to_indices[group_name] = [gpu_index]
                    except (ValueError, IndexError):
                        logger.warning("Could not extract GPU index from group name", LogContext(
                            operation="map_groups_to_indices",
                            gpu_group=group_name
                        ))
                        # For groups we can't map, assume they might use multiple GPUs
                        group_to_indices[group_name] = []
                else:
                    # For groups with different naming patterns, could be mapped manually
                    # or through configuration if needed
                    group_to_indices[group_name] = []
            
            logger.debug("Generated GPU group to indices mapping", LogContext(
                operation="map_groups_to_indices",
                extra_data={"mapping": group_to_indices}
            ))
            return group_to_indices
            
        except Exception as e:
            logger.error_with_exception("Failed to map GPU groups to indices", e, LogContext(
                operation="map_groups_to_indices"
            ))
            return {}

    def get_optimal_gpu_assignment(self) -> Optional[str]:
        """
        Get the optimal GPU group for assigning a new case.

        Analyzes current utilization, database availability, and actual hardware
        usage to determine the best GPU group for a new computational case.

        Returns:
            Name of the optimal GPU group, or None if no resources available
        """
        try:
            # Get current utilization from remote Pueue system
            pueue_utilization = self.get_gpu_resource_utilization()
            
            # Get actual hardware utilization
            hardware_utilization = self.get_gpu_hardware_utilization()
            
            # Get mapping from groups to GPU indices
            group_to_indices = self.map_gpu_groups_to_indices()

            # Find available resources considering database status, Pueue utilization, AND hardware usage
            available_groups = []
            for group_name in pueue_utilization.keys():
                resource = self.db_manager.get_gpu_resource(group_name)
                
                # Check database availability first
                if not resource or resource["status"] != "available":
                    if resource:
                        logger.debug("GPU unavailable in database", LogContext(
                            operation="get_optimal_assignment",
                            gpu_group=group_name,
                            extra_data={"db_status": resource['status']}
                        ))
                    else:
                        logger.debug("GPU not found in database", LogContext(
                            operation="get_optimal_assignment",
                            gpu_group=group_name
                        ))
                    continue
                
                # Check Pueue queue utilization
                pueue_stats = pueue_utilization[group_name]
                if pueue_stats["running"] > 0:
                    logger.info("GPU busy with Pueue jobs", LogContext(
                        operation="get_optimal_assignment",
                        gpu_group=group_name,
                        extra_data={"running_jobs": pueue_stats['running']}
                    ))
                    continue
                
                # Check hardware utilization for this group's GPUs
                gpu_indices = group_to_indices.get(group_name, [])
                hardware_busy = False
                
                for gpu_index in gpu_indices:
                    if gpu_index in hardware_utilization:
                        hw_stats = hardware_utilization[gpu_index]
                        if hw_stats["is_busy"]:
                            logger.info("GPU hardware busy", LogContext(
                                operation="get_optimal_assignment",
                                gpu_group=group_name,
                                extra_data={
                                    "gpu_index": gpu_index,
                                    "utilization_percent": hw_stats['utilization_gpu'],
                                    "memory_percent": round(hw_stats['memory_percent'], 1)
                                }
                            ))
                            hardware_busy = True
                            break
                
                if hardware_busy:
                    continue
                
                # This group is truly available - add to candidates
                total_score = pueue_stats["total_load"]
                
                # Add hardware utilization to scoring (lower is better)
                for gpu_index in gpu_indices:
                    if gpu_index in hardware_utilization:
                        hw_stats = hardware_utilization[gpu_index]
                        # Add small penalty for any utilization to prefer completely idle GPUs
                        total_score += hw_stats["utilization_gpu"] / 100.0
                        total_score += hw_stats["memory_percent"] / 100.0
                
                available_groups.append((group_name, total_score))
                
                hw_info = ""
                if gpu_indices:
                    hw_details = []
                    for idx in gpu_indices:
                        if idx in hardware_utilization:
                            hw = hardware_utilization[idx]
                            hw_details.append(f"GPU{idx}: {hw['utilization_gpu']}%/{hw['memory_percent']:.1f}%")
                    hw_info = f", Hardware: [{', '.join(hw_details)}]"
                
                logger.debug("GPU available for assignment", LogContext(
                    operation="get_optimal_assignment",
                    gpu_group=group_name,
                    extra_data={
                        "pueue_running": pueue_stats['running'],
                        "hardware_info": hw_info,
                        "score": total_score
                    }
                ))

            if not available_groups:
                logger.warning("No GPU resources available for assignment", LogContext(
                    operation="get_optimal_assignment",
                    extra_data={"reason": "all resources assigned, running jobs, or hardware busy"}
                ))
                return None

            # Sort by total score (ascending) to get best available resource
            available_groups.sort(key=lambda x: x[1])
            optimal_group = available_groups[0][0]

            logger.info("Selected optimal GPU group for assignment", LogContext(
                operation="get_optimal_assignment",
                gpu_group=optimal_group,
                extra_data={"optimization_score": round(available_groups[0][1], 2)}
            ))
            return optimal_group

        except Exception as e:
            logger.error_with_exception("Failed to determine optimal GPU assignment", e, LogContext(
                operation="get_optimal_assignment"
            ))
            return None

    def update_db_status_from_hardware(
        self,
        pueue_utilization: Dict[str, Dict[str, int]],
        hardware_utilization: Dict[int, Dict[str, Any]],
        group_to_indices: Dict[str, List[int]],
    ) -> None:
        """
        Updates the database with the real-time hardware status of GPUs.

        It marks resources as 'busy' if they are being used by external processes
        or have running Pueue jobs, and 'available' if they are free. It does not
        touch resources that are 'assigned' or 'zombie' as they are managed by
        the main application loop.
        """
        logger.debug("Starting database update with real-time hardware status", LogContext(
            operation="update_db_from_hardware"
        ))
        all_db_resources = self.db_manager.get_all_gpu_resources()
        db_resources_map = {res["pueue_group"]: res for res in all_db_resources}

        all_groups = set(db_resources_map.keys()) | set(group_to_indices.keys())

        for group_name in all_groups:
            current_resource = db_resources_map.get(group_name)
            if not current_resource:
                continue  # Should have been created by sync_gpu_resources_with_database

            current_db_status = current_resource.get("status")

            if current_db_status in ["assigned", "zombie"]:
                continue

            # Determine if the group should be considered busy
            is_busy = False
            # 1. Check Pueue for running tasks in this group
            if pueue_utilization.get(group_name, {}).get("running", 0) > 0:
                is_busy = True

            # 2. Check hardware utilization via nvidia-smi
            if not is_busy:
                indices = group_to_indices.get(group_name, [])
                for index in indices:
                    if hardware_utilization.get(index, {}).get("is_busy", False):
                        is_busy = True
                        break

            new_status = "busy" if is_busy else "available"

            if new_status != current_db_status:
                self.db_manager.update_gpu_status(group_name, new_status, case_id=None)
                logger.info("GPU resource status updated based on real-time state", LogContext(
                    operation="update_db_from_hardware",
                    gpu_group=group_name,
                    extra_data={
                        "old_status": current_db_status,
                        "new_status": new_status
                    }
                ))

    def refresh_gpu_resources(self) -> Dict[str, Any]:
        """
        Perform a complete refresh of GPU resource information.

        This method combines detection, synchronization, and utilization
        analysis to provide a comprehensive update of GPU resource status.

        Returns:
            Dictionary containing detected groups, Pueue utilization, and hardware utilization
        """
        try:
            # Detect available groups
            detected_groups = self.detect_available_gpu_groups()

            # Sync with database to ensure all groups exist
            self.sync_gpu_resources_with_database()

            # Get current Pueue utilization
            pueue_utilization = self.get_gpu_resource_utilization()
            
            # Get current hardware utilization
            hardware_utilization = self.get_gpu_hardware_utilization()
            
            # Get group to indices mapping
            group_to_indices = self.map_gpu_groups_to_indices()

            # Update database status based on hardware and Pueue utilization
            self.update_db_status_from_hardware(
                pueue_utilization, hardware_utilization, group_to_indices
            )

            result = {
                "detected_groups": detected_groups, 
                "pueue_utilization": pueue_utilization,
                "hardware_utilization": hardware_utilization,
                "group_to_indices": group_to_indices
            }

            logger.info("GPU resource refresh completed successfully", LogContext(
                operation="refresh_resources",
                extra_data={
                    "detected_groups_count": len(result["detected_groups"]),
                    "hardware_gpus_count": len(result["hardware_utilization"])
                }
            ))
            return result

        except Exception as e:
            logger.error_with_exception("GPU resource refresh failed", e, LogContext(
                operation="refresh_resources"
            ))
            raise
