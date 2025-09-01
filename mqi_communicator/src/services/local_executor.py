import subprocess
import re
from pathlib import Path
from typing import Dict, Any, Optional, List
import logging

from src.common.structured_logging import get_structured_logger, LogContext
from src.common.error_categorization import BaseExecutionError
from src.common.error_categorization import categorize_error

logger = get_structured_logger(__name__)


class LocalExecutionError(BaseExecutionError):
    """Custom exception for errors during local execution."""
    
    def __init__(self, message: str, return_code: Optional[int] = None, stderr: Optional[str] = None):
        details = {}
        if return_code is not None:
            details["return_code"] = return_code
        if stderr:
            details["stderr"] = stderr
        super().__init__(message, details)


class LocalExecutor:
    """
    Handles the execution and monitoring of local scripts.
    
    This class is responsible for running local scripts like mqi_interpreter 
    and raw2dcm, with real-time output parsing for progress display.
    """

    def __init__(self, config: Dict[str, Any]) -> None:
        """
        Initialize the LocalExecutor with configuration.
        
        Args:
            config: Configuration dictionary containing local_tools settings
        """
        self.local_tools = config.get("local_tools", {})
        
    def execute(self, target: str, context: Dict[str, Any], display = None) -> Dict[str, Any]:
        """
        Execute a local script with real-time output monitoring.
        
        Args:
            target: The target script to execute (e.g., 'run_interpreter', 'run_raw2dcm')
            context: Execution context containing case information and parameters
            display: Optional RichProgressDisplay instance for UI updates
            
        Returns:
            Dictionary containing execution results and metadata
            
        Raises:
            LocalExecutionError: If execution fails
        """
        if target == "run_interpreter":
            return self._run_interpreter(context, display)
        elif target == "run_raw2dcm":
            return self._run_raw2dcm(context, display)
        else:
            raise LocalExecutionError(f"Unknown local execution target: {target}")

    def _run_interpreter(self, context: Dict[str, Any], display = None) -> Dict[str, Any]:
        """
        Run the local MQI interpreter script.
        
        Args:
            context: Execution context with case_path, case_id, etc.
            display: Optional progress display instance
            
        Returns:
            Dictionary with execution results
        """
        case_path = context["case_path"]
        case_id = context.get("case_id")
        
        script_path = self.local_tools.get("mqi_interpreter")
        if not script_path:
            raise LocalExecutionError("mqi_interpreter script path not configured")
            
        # Verify script exists
        if not Path(script_path).exists():
            raise LocalExecutionError(f"Interpreter script not found: {script_path}")
            
        # Build command
        command = [
            "python3", script_path,
            "--logdir", case_path,
            "--outputdir", case_path
        ]
        
        logger.info(
            "Starting local interpreter execution",
            context=LogContext(
                case_id=str(case_id) if case_id else None,
                operation="local_interpreter",
                extra_data={
                    "case_path": case_path,
                    "script_path": script_path
                }
            ).to_dict()
        )
        
        try:
            result = self._execute_with_monitoring(
                command, 
                "Local Interpreter",
                context,
                display
            )
            
            # Verify expected output files exist
            expected_output = Path(case_path) / "intermediate" / "results.bin"
            if not expected_output.exists():
                logger.warning(
                    "Expected interpreter output file not found",
                    context=LogContext(
                        case_id=str(case_id) if case_id else None,
                        operation="local_interpreter",
                        extra_data={"expected_file": str(expected_output)}
                    ).to_dict()
                )
            
            logger.info(
                "Local interpreter execution completed successfully",
                context=LogContext(
                    case_id=str(case_id) if case_id else None,
                    operation="local_interpreter",
                    extra_data={
                        "execution_time": result.get("execution_time_seconds"),
                        "output_lines": result.get("output_line_count")
                    }
                ).to_dict()
            )
            
            return result
            
        except Exception as e:
            error_category = categorize_error(e, "local_interpreter")
            logger.error_with_exception(
                "Local interpreter execution failed",
                e,
                context=LogContext(
                    case_id=str(case_id) if case_id else None,
                    operation="local_interpreter",
                    error_category=error_category,
                    extra_data={"case_path": case_path}
                ).to_dict()
            )
            raise

    def _run_raw2dcm(self, context: Dict[str, Any], display = None) -> Dict[str, Any]:
        """
        Run the local raw2dcm conversion script.
        
        Args:
            context: Execution context with case_path, case_id, etc.
            display: Optional progress display instance
            
        Returns:
            Dictionary with execution results
        """
        case_path = context["case_path"]
        case_id = context.get("case_id")
        
        script_path = self.local_tools.get("raw2dcm")
        if not script_path:
            raise LocalExecutionError("raw2dcm script path not configured")
            
        # Verify script exists
        if not Path(script_path).exists():
            raise LocalExecutionError(f"Raw2DCM script not found: {script_path}")
            
        # Check for input files in raw_output directory
        raw_output_dir = Path(case_path) / "raw_output"
        if not raw_output_dir.exists():
            raise LocalExecutionError(f"Raw output directory not found: {raw_output_dir}")
            
        raw_files = list(raw_output_dir.glob("*.raw"))
        if not raw_files:
            raise LocalExecutionError(f"No .raw files found in {raw_output_dir}")
        
        # Build command (assuming script takes input and output directories)
        output_dir = Path(case_path) / "final_dcm"
        output_dir.mkdir(parents=True, exist_ok=True)
        
        command = [
            "python3", script_path,
            "--input", str(raw_output_dir),
            "--output", str(output_dir)
        ]
        
        logger.info(
            "Starting local raw2dcm conversion",
            context=LogContext(
                case_id=str(case_id) if case_id else None,
                operation="local_raw2dcm",
                extra_data={
                    "case_path": case_path,
                    "script_path": script_path,
                    "input_files": len(raw_files)
                }
            ).to_dict()
        )
        
        try:
            result = self._execute_with_monitoring(
                command,
                "Raw2DCM Conversion", 
                context,
                display
            )
            
            # Verify output files were created
            dcm_files = list(output_dir.glob("*.dcm"))
            
            logger.info(
                "Local raw2dcm conversion completed successfully",
                context=LogContext(
                    case_id=str(case_id) if case_id else None,
                    operation="local_raw2dcm",
                    extra_data={
                        "execution_time": result.get("execution_time_seconds"),
                        "output_dcm_files": len(dcm_files)
                    }
                ).to_dict()
            )
            
            result["output_files"] = [str(f) for f in dcm_files]
            return result
            
        except Exception as e:
            error_category = categorize_error(e, "local_raw2dcm")
            logger.error_with_exception(
                "Local raw2dcm conversion failed",
                e,
                context=LogContext(
                    case_id=str(case_id) if case_id else None,
                    operation="local_raw2dcm",
                    error_category=error_category,
                    extra_data={"case_path": case_path}
                ).to_dict()
            )
            raise

    def _execute_with_monitoring(
        self, 
        command: List[str], 
        step_name: str,
        context: Dict[str, Any],
        display = None
    ) -> Dict[str, Any]:
        """
        Execute a command with real-time output monitoring and parsing.
        
        Args:
            command: Command list to execute
            step_name: Human-readable name for the step
            context: Execution context
            display: Optional progress display instance
            
        Returns:
            Dictionary containing execution results and metadata
        """
        import time
        
        start_time = time.time()
        output_lines = []
        error_lines = []
        
        if display:
            display.start_step(step_name)
        
        try:
            process = subprocess.Popen(
                command,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                bufsize=1,
                universal_newlines=True
            )
            
            # Read output line by line for real-time processing
            while True:
                stdout_line = process.stdout.readline()
                stderr_line = process.stderr.readline()
                
                if stdout_line:
                    line = stdout_line.rstrip()
                    output_lines.append(line)
                    
                    # Parse standardized output for progress
                    self._parse_output_line(line, display)
                    
                    if display:
                        display.log_console_output(line, "stdout")
                        
                if stderr_line:
                    line = stderr_line.rstrip()
                    error_lines.append(line)
                    
                    if display:
                        display.log_console_output(line, "stderr")
                
                # Check if process has finished
                if process.poll() is not None:
                    # Read any remaining output
                    remaining_stdout = process.stdout.read()
                    remaining_stderr = process.stderr.read()
                    
                    if remaining_stdout:
                        for line in remaining_stdout.rstrip().split('\n'):
                            if line:
                                output_lines.append(line)
                                self._parse_output_line(line, display)
                                if display:
                                    display.log_console_output(line, "stdout")
                                    
                    if remaining_stderr:
                        for line in remaining_stderr.rstrip().split('\n'):
                            if line:
                                error_lines.append(line)
                                if display:
                                    display.log_console_output(line, "stderr")
                    break
                    
                if not stdout_line and not stderr_line and process.poll() is not None:
                    break
            
            return_code = process.wait()
            execution_time = time.time() - start_time
            
            if return_code != 0:
                error_msg = f"{step_name} failed with return code {return_code}"
                if error_lines:
                    error_msg += f"\nStderr: {'; '.join(error_lines[-5:])}"  # Last 5 error lines
                    
                if display:
                    display.set_error(error_msg)
                    
                raise LocalExecutionError(
                    error_msg,
                    return_code=return_code,
                    stderr='\n'.join(error_lines)
                )
            
            if display:
                display.complete_step()
                
            return {
                "success": True,
                "return_code": return_code,
                "execution_time_seconds": execution_time,
                "output_line_count": len(output_lines),
                "stdout_lines": output_lines,
                "stderr_lines": error_lines
            }
            
        except subprocess.SubprocessError as e:
            error_msg = f"Failed to execute {step_name}: {str(e)}"
            
            if display:
                display.set_error(error_msg)
                
            raise LocalExecutionError(error_msg) from e

    def _parse_output_line(self, line: str, display = None) -> None:
        """
        Parse a line of output for standardized progress indicators.
        
        Looks for patterns like:
        - STATUS:: <message>
        - PROGRESS:: <0-100>
        - SUBTASK:: <message>
        
        Args:
            line: Output line to parse
            display: Optional progress display to update
        """
        if not display:
            return
            
        # STATUS messages
        status_match = re.match(r'^STATUS::\s*(.+)', line)
        if status_match:
            status_msg = status_match.group(1).strip()
            display.update_status(status_msg)
            return
            
        # PROGRESS percentage
        progress_match = re.match(r'^PROGRESS::\s*(\d+)', line)
        if progress_match:
            try:
                progress = int(progress_match.group(1))
                if 0 <= progress <= 100:
                    display.update_progress(progress)
            except ValueError:
                pass
            return
            
        # SUBTASK messages
        subtask_match = re.match(r'^SUBTASK::\s*(.+)', line)
        if subtask_match:
            subtask_msg = subtask_match.group(1).strip()
            display.update_subtask(subtask_msg)
            return