"""
Handles local CLI execution (P2, P3).
Manages execution of external command-line tools with robust error handling.
"""
import subprocess
from typing import NamedTuple, List, Dict
from pathlib import Path
from .config import Config

class ExecutionResult(NamedTuple):
    """
    Structured result of a subprocess execution.
    """
    success: bool
    output: str
    error: str
    return_code: int


class LocalHandler:
    """
    Handler for executing local command-line tools.
    """
    def __init__(self, config: Config):
        self.config = config
        self.python_interpreter = self.config.executables.python_interpreter

    def _execute_subprocess(self, command: List[str], timeout: int = 300) -> ExecutionResult:
        """Executes a command in a subprocess with a timeout and captures output."""
        try:
            process = subprocess.run(
                command,
                capture_output=True,
                text=True,
                check=False,
                timeout=timeout
            )
            success = process.returncode == 0
            return ExecutionResult(
                success=success,
                output=process.stdout,
                error=process.stderr,
                return_code=process.returncode
            )
        except FileNotFoundError:
            return ExecutionResult(False, "", f"Executable not found: {command[0]}", -1)
        except subprocess.TimeoutExpired:
            return ExecutionResult(False, "", f"Command timed out after {timeout} seconds.", -1)
        except Exception as e:
            return ExecutionResult(False, "", f"An unexpected error occurred: {e}", -1)

    def execute_mqi_interpreter(self, case_id: str, case_path: Path) -> ExecutionResult:
        """
        Execute the mqi_interpreter (P2) for a specific case.
        """
        mqi_interpreter_script = self.config.executables.mqi_interpreter
        processing_dir = Path(self.config.paths.local.processing_directory.format(case_id=case_id))
        processing_dir.mkdir(parents=True, exist_ok=True)

        command = [
            self.python_interpreter,
            mqi_interpreter_script,
            "--dicom_input_folder", str(case_path),
            "--output_folder", str(processing_dir)
        ]

        return self._execute_subprocess(command)

    def execute_raw_to_dicom(self, case_id: str) -> ExecutionResult:
        """
        Execute the RawToDCM converter (P3) for a specific case.
        """
        raw_to_dicom_script = self.config.executables.raw_to_dicom
        raw_output_dir = Path(self.config.paths.local.raw_output_directory.format(case_id=case_id))
        final_dicom_dir = Path(self.config.paths.local.final_dicom_directory.format(case_id=case_id))
        final_dicom_dir.mkdir(parents=True, exist_ok=True)

        # Assuming the raw file is named 'dose.raw' as per the goal document
        raw_file_path = raw_output_dir / "dose.raw"
        if not raw_file_path.exists():
            return ExecutionResult(False, "", f"Raw file not found: {raw_file_path}", -1)

        command = [
            self.python_interpreter,
            raw_to_dicom_script,
            "--input_raw_file", str(raw_file_path),
            "--output_dicom_folder", str(final_dicom_dir)
        ]
        
        result = self._execute_subprocess(command)

        # Verify that DICOM files were created
        if result.success and not any(final_dicom_dir.glob("*.dcm")):
            return ExecutionResult(
                False,
                result.output,
                "RawToDCM executed successfully, but no DICOM files were created.",
                result.return_code
            )
            
        return result