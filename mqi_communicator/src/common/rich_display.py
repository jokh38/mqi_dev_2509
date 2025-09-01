import time
from typing import Optional, Dict, Any, List
from dataclasses import dataclass
from datetime import datetime

try:
    from rich.console import Console
    from rich.progress import Progress, TaskID, SpinnerColumn, TextColumn, BarColumn, TimeElapsedColumn
    from rich.live import Live
    from rich.panel import Panel
    from rich.table import Table
    from rich.text import Text
    from rich.layout import Layout
    from rich import box
    RICH_AVAILABLE = True
except ImportError:
    RICH_AVAILABLE = False

from src.common.structured_logging import get_structured_logger, LogContext

logger = get_structured_logger(__name__)


@dataclass
class StepInfo:
    """Information about a workflow step."""
    name: str
    status: str = "pending"  # pending, running, completed, failed
    progress: int = 0
    current_subtask: str = ""
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    console_output: List[str] = None
    error_message: str = ""
    
    def __post_init__(self):
        if self.console_output is None:
            self.console_output = []


class RichProgressDisplay:
    """
    A rich display manager for real-time progress visualization.
    
    This class encapsulates all UI logic for displaying workflow progress
    using the rich library, providing methods for step management,
    progress updates, and console output logging.
    """

    def __init__(self, case_name: str = "Unknown Case", case_id: Optional[int] = None):
        """
        Initialize the progress display.
        
        Args:
            case_name: Name of the case being processed
            case_id: Optional database ID of the case
        """
        self.case_name = case_name
        self.case_id = case_id
        self.console = Console() if RICH_AVAILABLE else None
        self.steps: Dict[str, StepInfo] = {}
        self.current_step: Optional[str] = None
        self.live_display: Optional[Live] = None
        self.progress: Optional[Progress] = None
        self.current_task_id: Optional[TaskID] = None
        
        # Console output buffer (limited to prevent memory issues)
        self.max_console_lines = 100
        
        if not RICH_AVAILABLE:
            logger.warning(
                "Rich library not available - falling back to basic text output",
                context=LogContext(
                    operation="display_initialization",
                    extra_data={"case_name": case_name, "case_id": case_id}
                ).to_dict()
            )

    def __enter__(self):
        """Context manager entry."""
        if RICH_AVAILABLE:
            self.progress = Progress(
                SpinnerColumn(),
                TextColumn("[bold blue]{task.description}"),
                BarColumn(),
                TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
                TimeElapsedColumn(),
                console=self.console
            )
            self.live_display = Live(
                self._create_display_layout(),
                console=self.console,
                refresh_per_second=2
            )
            self.live_display.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        if self.live_display:
            self.live_display.stop()

    def _create_display_layout(self) -> Layout:
        """Create the main display layout."""
        if not RICH_AVAILABLE:
            return None
            
        layout = Layout()
        layout.split_column(
            Layout(self._create_header_panel(), size=5, name="header"),
            Layout(self._create_progress_panel(), size=8, name="progress"),
            Layout(self._create_console_panel(), name="console")
        )
        return layout

    def _create_header_panel(self) -> Panel:
        """Create the header panel with case information."""
        if not RICH_AVAILABLE:
            return None
            
        case_info = f"Case: {self.case_name}"
        if self.case_id:
            case_info += f" (ID: {self.case_id})"
            
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        header_text = Text()
        header_text.append(case_info, style="bold cyan")
        header_text.append(f"\nStarted: {current_time}", style="dim")
        
        if self.current_step:
            header_text.append(f"\nCurrent Step: {self.current_step}", style="yellow")
        
        return Panel(
            header_text,
            title="MQI Communicator - Workflow Progress",
            border_style="blue"
        )

    def _create_progress_panel(self) -> Panel:
        """Create the progress panel showing step information."""
        if not RICH_AVAILABLE:
            return Panel("Progress panel not available")
            
        if not self.steps:
            return Panel("No steps to display", title="Progress")
            
        table = Table(box=box.MINIMAL)
        table.add_column("Step", style="bold")
        table.add_column("Status", justify="center")
        table.add_column("Progress", justify="center")
        table.add_column("Details", style="dim")
        
        for step_name, step_info in self.steps.items():
            # Status with color coding
            if step_info.status == "completed":
                status = "[green]✓ Completed[/green]"
            elif step_info.status == "running":
                status = "[yellow]⚡ Running[/yellow]"
            elif step_info.status == "failed":
                status = "[red]✗ Failed[/red]"
            else:
                status = "[dim]○ Pending[/dim]"
            
            # Progress bar or percentage
            if step_info.progress > 0:
                progress_text = f"{step_info.progress}%"
            else:
                progress_text = "-"
            
            # Details (current subtask or error)
            details = step_info.current_subtask or step_info.error_message
            if len(details) > 40:
                details = details[:37] + "..."
            
            table.add_row(step_name, status, progress_text, details)
        
        return Panel(table, title="Workflow Steps", border_style="yellow")

    def _create_console_panel(self) -> Panel:
        """Create the console output panel."""
        if not RICH_AVAILABLE:
            return Panel("Console panel not available")
            
        if not self.current_step or self.current_step not in self.steps:
            return Panel("No console output", title="Console Output")
        
        step_info = self.steps[self.current_step]
        
        # Show last N lines of console output
        console_lines = step_info.console_output[-20:] if step_info.console_output else []
        
        console_text = Text()
        for line in console_lines:
            # Simple color coding based on content
            if "ERROR" in line.upper() or "FAILED" in line.upper():
                console_text.append(line + "\n", style="red")
            elif "WARNING" in line.upper() or "WARN" in line.upper():
                console_text.append(line + "\n", style="yellow")
            elif "STATUS::" in line:
                console_text.append(line + "\n", style="blue")
            elif "PROGRESS::" in line:
                console_text.append(line + "\n", style="green")
            elif "SUBTASK::" in line:
                console_text.append(line + "\n", style="cyan")
            else:
                console_text.append(line + "\n", style="white")
        
        if not console_lines:
            console_text.append("No output yet...", style="dim")
        
        return Panel(
            console_text,
            title=f"Console Output - {self.current_step}",
            border_style="green"
        )

    def add_step(self, step_name: str, description: str = None) -> None:
        """
        Add a new step to the workflow.
        
        Args:
            step_name: Unique identifier for the step
            description: Optional human-readable description
        """
        self.steps[step_name] = StepInfo(
            name=description or step_name,
            status="pending"
        )
        
        self._update_display()
        
        logger.debug(
            f"Added workflow step: {step_name}",
            context=LogContext(
                operation="step_addition",
                extra_data={"step_name": step_name, "description": description}
            ).to_dict()
        )

    def start_step(self, step_name: str) -> None:
        """
        Mark a step as started and set it as the current step.
        
        Args:
            step_name: Name of the step to start
        """
        if step_name not in self.steps:
            self.add_step(step_name)
        
        self.steps[step_name].status = "running"
        self.steps[step_name].start_time = datetime.now()
        self.current_step = step_name
        
        if RICH_AVAILABLE and self.progress:
            self.current_task_id = self.progress.add_task(
                description=self.steps[step_name].name,
                total=100
            )
        
        self._update_display()
        
        logger.info(
            f"Started workflow step: {step_name}",
            context=LogContext(
                operation="step_start",
                extra_data={"step_name": step_name, "case_name": self.case_name}
            ).to_dict()
        )

    def update_progress(self, progress: int) -> None:
        """
        Update the progress of the current step.
        
        Args:
            progress: Progress percentage (0-100)
        """
        if not self.current_step:
            return
        
        progress = max(0, min(100, progress))  # Clamp to 0-100
        self.steps[self.current_step].progress = progress
        
        if RICH_AVAILABLE and self.progress and self.current_task_id:
            self.progress.update(self.current_task_id, completed=progress)
        
        self._update_display()

    def update_status(self, status_message: str) -> None:
        """
        Update the status message for the current step.
        
        Args:
            status_message: Status message to display
        """
        if not self.current_step:
            return
        
        self.steps[self.current_step].current_subtask = status_message
        self._update_display()
        
        # Also log to console output
        self.log_console_output(f"STATUS:: {status_message}", "status")

    def update_subtask(self, subtask_message: str) -> None:
        """
        Update the current subtask for the current step.
        
        Args:
            subtask_message: Subtask message to display
        """
        if not self.current_step:
            return
        
        self.steps[self.current_step].current_subtask = subtask_message
        self._update_display()
        
        # Also log to console output
        self.log_console_output(f"SUBTASK:: {subtask_message}", "subtask")

    def log_console_output(self, line: str, output_type: str = "stdout") -> None:
        """
        Add a line to the console output buffer.
        
        Args:
            line: Line of output to add
            output_type: Type of output (stdout, stderr, status, subtask)
        """
        if not self.current_step:
            return
        
        step_info = self.steps[self.current_step]
        
        # Add timestamp prefix for non-structured output
        if not any(prefix in line for prefix in ["STATUS::", "PROGRESS::", "SUBTASK::"]):
            timestamp = datetime.now().strftime("%H:%M:%S")
            line = f"[{timestamp}] {line}"
        
        step_info.console_output.append(line)
        
        # Limit console output to prevent memory issues
        if len(step_info.console_output) > self.max_console_lines:
            step_info.console_output = step_info.console_output[-self.max_console_lines:]
        
        self._update_display()

    def complete_step(self) -> None:
        """Mark the current step as completed."""
        if not self.current_step:
            return
        
        self.steps[self.current_step].status = "completed"
        self.steps[self.current_step].progress = 100
        self.steps[self.current_step].end_time = datetime.now()
        
        if RICH_AVAILABLE and self.progress and self.current_task_id:
            self.progress.update(self.current_task_id, completed=100)
            self.progress.remove_task(self.current_task_id)
            self.current_task_id = None
        
        self._update_display()
        
        logger.info(
            f"Completed workflow step: {self.current_step}",
            context=LogContext(
                operation="step_completion",
                extra_data={
                    "step_name": self.current_step,
                    "execution_time": self._get_step_duration(self.current_step)
                }
            ).to_dict()
        )
        
        self.current_step = None

    def set_error(self, error_message: str) -> None:
        """
        Mark the current step as failed with an error message.
        
        Args:
            error_message: Error message to display
        """
        if not self.current_step:
            return
        
        self.steps[self.current_step].status = "failed"
        self.steps[self.current_step].error_message = error_message
        self.steps[self.current_step].end_time = datetime.now()
        
        if RICH_AVAILABLE and self.progress and self.current_task_id:
            self.progress.remove_task(self.current_task_id)
            self.current_task_id = None
        
        self._update_display()
        
        logger.error(
            f"Failed workflow step: {self.current_step} - {error_message}",
            context=LogContext(
                operation="step_failure",
                extra_data={
                    "step_name": self.current_step,
                    "error_message": error_message
                }
            ).to_dict()
        )

    def _update_display(self) -> None:
        """Update the live display if rich is available."""
        if RICH_AVAILABLE and self.live_display:
            try:
                self.live_display.update(self._create_display_layout())
            except Exception as e:
                # Don't let display errors break the workflow
                logger.debug(f"Display update error: {e}")

    def _get_step_duration(self, step_name: str) -> Optional[float]:
        """Get the duration of a step in seconds."""
        if step_name not in self.steps:
            return None
        
        step_info = self.steps[step_name]
        if not step_info.start_time:
            return None
        
        end_time = step_info.end_time or datetime.now()
        return (end_time - step_info.start_time).total_seconds()

    def get_summary(self) -> Dict[str, Any]:
        """
        Get a summary of the workflow execution.
        
        Returns:
            Dictionary containing execution summary
        """
        total_steps = len(self.steps)
        completed_steps = sum(1 for step in self.steps.values() if step.status == "completed")
        failed_steps = sum(1 for step in self.steps.values() if step.status == "failed")
        
        total_duration = 0
        for step_info in self.steps.values():
            duration = self._get_step_duration(step_info.name)
            if duration:
                total_duration += duration
        
        return {
            "case_name": self.case_name,
            "case_id": self.case_id,
            "total_steps": total_steps,
            "completed_steps": completed_steps,
            "failed_steps": failed_steps,
            "success_rate": (completed_steps / total_steps * 100) if total_steps > 0 else 0,
            "total_duration_seconds": total_duration,
            "steps": {
                name: {
                    "status": info.status,
                    "progress": info.progress,
                    "duration_seconds": self._get_step_duration(name),
                    "error": info.error_message if info.error_message else None
                }
                for name, info in self.steps.items()
            }
        }


class FallbackDisplay:
    """
    A fallback display class for when rich is not available.
    
    Provides the same interface but outputs to standard logging/print.
    """

    def __init__(self, case_name: str = "Unknown Case", case_id: Optional[int] = None):
        self.case_name = case_name
        self.case_id = case_id
        self.current_step = None
        
    def __enter__(self):
        logger.info(f"Starting workflow for case: {self.case_name}", LogContext(
            operation="workflow_start",
            extra_data={"case_name": self.case_name, "case_id": self.case_id}
        ))
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        status = "failed" if exc_type else "completed"
        logger.info(f"Workflow {status} for case: {self.case_name}", LogContext(
            operation="workflow_end",
            extra_data={"case_name": self.case_name, "case_id": self.case_id, "status": status}
        ))

    def add_step(self, step_name: str, description: str = None) -> None:
        pass  # No need to pre-add steps in fallback mode

    def start_step(self, step_name: str) -> None:
        self.current_step = step_name
        logger.info(f"Starting step: {step_name}", LogContext(
            operation="step_start",
            extra_data={"case_name": self.case_name, "step_name": step_name}
        ))

    def update_progress(self, progress: int) -> None:
        logger.info(f"Progress: {progress}%", LogContext(
            operation="progress_update",
            extra_data={"case_name": self.case_name, "progress": progress}
        ))

    def update_status(self, status_message: str) -> None:
        logger.info(f"Status: {status_message}", LogContext(
            operation="status_update",
            extra_data={"case_name": self.case_name, "status": status_message}
        ))

    def update_subtask(self, subtask_message: str) -> None:
        logger.info(f"Subtask: {subtask_message}", LogContext(
            operation="subtask_update",
            extra_data={"case_name": self.case_name, "subtask": subtask_message}
        ))

    def log_console_output(self, line: str, output_type: str = "stdout") -> None:
        # Only log important output to avoid spam
        if any(prefix in line for prefix in ["STATUS::", "PROGRESS::", "SUBTASK::", "ERROR", "WARNING"]):
            if "ERROR" in line or "WARNING" in line:
                logger.warning(f"Console output: {line}", LogContext(
                    operation="console_output",
                    extra_data={"case_name": self.case_name, "output_type": output_type}
                ))
            else:
                logger.info(f"Console output: {line}", LogContext(
                    operation="console_output",
                    extra_data={"case_name": self.case_name, "output_type": output_type}
                ))

    def complete_step(self) -> None:
        if self.current_step:
            logger.info(f"Completed step: {self.current_step}", LogContext(
                operation="step_complete",
                extra_data={"case_name": self.case_name, "step_name": self.current_step}
            ))
            self.current_step = None

    def set_error(self, error_message: str) -> None:
        if self.current_step:
            logger.error(f"ERROR in {self.current_step}: {error_message}", LogContext(
                operation="step_error",
                extra_data={"case_name": self.case_name, "step_name": self.current_step, "error": error_message}
            ))

    def get_summary(self) -> Dict[str, Any]:
        return {
            "case_name": self.case_name,
            "case_id": self.case_id,
            "fallback_mode": True
        }


def create_progress_display(case_name: str, case_id: Optional[int] = None):
    """
    Factory function to create appropriate display based on rich availability.
    
    Args:
        case_name: Name of the case being processed
        case_id: Optional database ID of the case
        
    Returns:
        RichProgressDisplay if rich is available, otherwise FallbackDisplay
    """
    if RICH_AVAILABLE:
        return RichProgressDisplay(case_name, case_id)
    else:
        return FallbackDisplay(case_name, case_id)