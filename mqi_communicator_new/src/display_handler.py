"""
Manages rich console display for progress.
Provides real-time status dashboard using rich library.
"""
import threading
import time
from collections import deque
from typing import Dict, Any, List, Tuple

from rich.console import Console
from rich.layout import Layout
from rich.live import Live
from rich.panel import Panel
from rich.table import Table
from rich.text import Text
from rich.progress import Progress, BarColumn, TextColumn, TimeElapsedColumn


class DisplayHandler:
    """
    Handler for rich console display and status dashboard.
    """

    def __init__(self, max_log_entries: int = 10):
        self.console = Console()
        self.layout = self._create_layout()
        self.live = Live(self.layout, console=self.console, screen=True, auto_refresh=False)

        self.active_cases: Dict[str, Any] = {}
        self.system_status: Dict[str, Any] = {"Active Workers": 0, "Queued Cases": 0}
        self.activity_log = deque(maxlen=max_log_entries)

        self._thread = threading.Thread(target=self._display_loop, daemon=True)
        self._stop_event = threading.Event()
        self._lock = threading.Lock()

    def _create_layout(self) -> Layout:
        """Defines the console layout."""
        layout = Layout()
        layout.split(
            Layout(name="header", size=3),
            Layout(ratio=1, name="main"),
            Layout(size=10, name="footer"),
        )
        layout["main"].split_row(Layout(name="side"), Layout(name="body", ratio=2))
        layout["side"].split(Layout(name="status"), Layout(name="cases"))
        layout["footer"].split_row(Layout(name="log", name="Log"), Layout(name="blank"))
        return layout

    def _create_header(self) -> Panel:
        """Creates the header panel."""
        header = Text("MQI Communicator Dashboard", justify="center", style="bold magenta")
        return Panel(header, title="[bold white]Real-time Status[/bold white]")

    def _create_system_panel(self) -> Panel:
        """Creates the system status panel."""
        status_text = Text()
        for key, value in self.system_status.items():
            status_text.append(f"{key}: ", style="bold green")
            status_text.append(str(value) + "\n")
        return Panel(status_text, title="[bold]System Info[/bold]")

    def _create_cases_table(self) -> Panel:
        """Creates the active cases table."""
        table = Table(
            TextColumn("[bold cyan]{task.description}"),
            BarColumn(),
            TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
            TimeElapsedColumn(),
            expand=True
        )

        for case_id, case_data in self.active_cases.items():
            table.add_task(
                description=case_id,
                total=case_data["total"],
                completed=case_data["progress"]
            )
        return Panel(table, title="[bold]Active Cases[/bold]")

    def _create_activity_log(self) -> Panel:
        """Creates the recent activity log panel."""
        log_text = Text("\n".join(self.activity_log), style="italic")
        return Panel(log_text, title="[bold]Recent Activity[/bold]")

    def update_display(self):
        """Updates the live display with current data."""
        with self._lock:
            self.layout["header"].update(self._create_header())
            self.layout["status"].update(self._create_system_panel())
            self.layout["cases"].update(self._create_cases_table())
            self.layout["log"].update(self._create_activity_log())
        self.live.refresh()

    def _display_loop(self):
        """The main loop for the display thread."""
        while not self._stop_event.is_set():
            self.update_display()
            time.sleep(0.5)

    def start(self):
        """Starts the live display in a separate thread."""
        self.live.start(refresh=True)
        self._thread.start()
        self.add_log_entry("Display handler started.")

    def stop(self):
        """Stops the live display thread."""
        self.add_log_entry("Shutting down display handler...")
        self._stop_event.set()
        self._thread.join()
        self.live.stop()
        self.console.clear()
        self.console.print("[bold green]Dashboard stopped.[/bold green]")

    def update_system_status(self, active_workers: int, queued_cases: int):
        """Updates the system status information."""
        with self._lock:
            self.system_status["Active Workers"] = active_workers
            self.system_status["Queued Cases"] = queued_cases

    def add_case(self, case_id: str):
        """Adds a new case to the display."""
        with self._lock:
            if case_id not in self.active_cases:
                self.active_cases[case_id] = {"progress": 0, "total": 100, "status": "Initializing"}
                self.add_log_entry(f"Case {case_id} added to dashboard.")

    def update_case_progress(self, case_id: str, status: str, progress: int):
        """Updates the progress of a case."""
        with self._lock:
            if case_id in self.active_cases:
                self.active_cases[case_id]["progress"] = progress
                self.active_cases[case_id]["status"] = status
                self.add_log_entry(f"Case {case_id}: {status} ({progress}%)")

    def remove_case(self, case_id: str, final_status: str):
        """Removes a completed or failed case from the active display."""
        with self._lock:
            if case_id in self.active_cases:
                del self.active_cases[case_id]
                self.add_log_entry(f"Case {case_id} finished with status: {final_status}.")

    def add_log_entry(self, message: str):
        """Adds a new entry to the activity log."""
        with self._lock:
            timestamp = time.strftime("%H:%M:%S")
            self.activity_log.append(f"[{timestamp}] {message}")