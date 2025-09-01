"""
This module provides a CLI dashboard to display the status of the MQI Communicator.

It uses the 'rich' library to render tables and live updates, showing the
state of active cases and GPU resources from the database.
"""
import time
import sys
import os
import json
import csv
from pathlib import Path
from rich.console import Console
from rich.live import Live
from rich.table import Table
from rich.layout import Layout
from rich.panel import Panel
from rich.align import Align
from rich.prompt import Prompt
from datetime import datetime
from typing import List, Dict, Any, Optional

# Add the parent directory to the path to import from src.common
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.common.config_manager import ConfigManager, ConfigValidationError
from src.common.db_manager import DatabaseManager, KST
from src.common.structured_logging import get_structured_logger, LogContext

# Initialize structured logger
logger = get_structured_logger(__name__)

# Define the path to the configuration file
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CONFIG_PATH = os.path.join(PROJECT_ROOT, "config", "config.yaml")

class DashboardFilter:
    """Filter configuration for dashboard data filtering and searching."""

    def __init__(
        self,
        status_filter: Optional[str] = None,
        gpu_group_filter: Optional[str] = None,
        date_from: Optional[datetime] = None,
        date_to: Optional[datetime] = None,
        search_term: str = "",
    ):
        self.status_filter = status_filter
        self.gpu_group_filter = gpu_group_filter
        self.date_from = date_from
        self.date_to = date_to
        self.search_term = search_term


def filter_cases(
    cases: List[Dict[str, Any]], filter_obj: DashboardFilter
) -> List[Dict[str, Any]]:
    """Filter cases based on status, GPU group, and date range."""
    filtered_cases = cases.copy()

    # Filter by status
    if filter_obj.status_filter:
        filtered_cases = [
            case
            for case in filtered_cases
            if case.get("status") == filter_obj.status_filter
        ]

    # Filter by GPU group
    if filter_obj.gpu_group_filter:
        filtered_cases = [
            case
            for case in filtered_cases
            if case.get("pueue_group") == filter_obj.gpu_group_filter
        ]

    # Filter by date range
    if filter_obj.date_from or filter_obj.date_to:
        date_filtered_cases = []
        for case in filtered_cases:
            try:
                case_date = datetime.strptime(
                    case.get("submitted_at", ""), "%Y-%m-%d %H:%M:%S"
                )
                if filter_obj.date_from and case_date < filter_obj.date_from:
                    continue
                if filter_obj.date_to and case_date > filter_obj.date_to:
                    continue
                date_filtered_cases.append(case)
            except (ValueError, TypeError):
                # If date parsing fails, skip the case
                continue
        filtered_cases = date_filtered_cases

    return filtered_cases


def search_cases(
    cases: List[Dict[str, Any]], filter_obj: DashboardFilter
) -> List[Dict[str, Any]]:
    """Search cases by case ID or case path."""
    if not filter_obj.search_term:
        return cases

    search_term = filter_obj.search_term.lower()
    searched_cases = []

    for case in cases:
        # Search in case path
        case_path = str(case.get("case_path", "")).lower()
        # Search in case ID
        case_id = str(case.get("case_id", ""))

        if search_term in case_path or search_term in case_id:
            searched_cases.append(case)

    return searched_cases


def export_to_csv(cases: List[Dict[str, Any]], file_path: str) -> None:
    """Export cases data to CSV file."""
    if not cases:
        return

    # Define the field names for CSV export
    fieldnames = [
        "case_id",
        "case_path",
        "status",
        "progress",
        "pueue_group",
        "pueue_task_id",
        "submitted_at",
        "status_updated_at",
    ]

    with open(file_path, "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()

        for case in cases:
            # Create a filtered dictionary with only the fieldnames
            filtered_case = {field: case.get(field, "") for field in fieldnames}
            writer.writerow(filtered_case)


def export_to_json(
    cases: List[Dict[str, Any]], resources: List[Dict[str, Any]], file_path: str
) -> None:
    """Export cases and resources data to JSON file."""
    export_data = {
        "exported_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "cases": cases,
        "resources": resources,
    }

    with open(file_path, "w", encoding="utf-8") as jsonfile:
        json.dump(export_data, jsonfile, indent=2, ensure_ascii=False)


def format_dashboard_snapshot(
    cases: List[Dict[str, Any]], resources: List[Dict[str, Any]]
) -> str:
    """Format dashboard data as a text snapshot."""
    snapshot_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    lines = []
    lines.append("=" * 60)
    lines.append("MQI Communicator Dashboard Snapshot")
    lines.append(f"Generated at: {snapshot_time}")
    lines.append("=" * 60)

    # Case Summary
    lines.append("\nCase Summary:")
    lines.append("-" * 40)
    if not cases:
        lines.append("No cases found.")
    else:
        status_counts = {}
        for case in cases:
            status = case.get("status", "unknown")
            status_counts[status] = status_counts.get(status, 0) + 1

        for status, count in sorted(status_counts.items()):
            lines.append(f"{status.capitalize()}: {count}")

        lines.append(f"Total Cases: {len(cases)}")

    # Resource Summary
    lines.append("\nResource Summary:")
    lines.append("-" * 40)
    if not resources:
        lines.append("No resources found.")
    else:
        available_count = sum(1 for r in resources if r.get("status") == "available")
        assigned_count = len(resources) - available_count

        lines.append(f"Available Resources: {available_count}")
        lines.append(f"Assigned Resources: {assigned_count}")
        lines.append(f"Total Resources: {len(resources)}")

    # Detailed Cases
    if cases:
        lines.append("\nDetailed Cases:")
        lines.append("-" * 40)
        for case in cases:
            lines.append(
                f"ID: {case.get('case_id', 'N/A')} | "
                f"Status: {case.get('status', 'N/A')} | "
                f"Path: {case.get('case_path', 'N/A')} | "
                f"GPU: {case.get('pueue_group', 'N/A')} | "
                f"Progress: {case.get('progress', 0)}%"
            )

    lines.append("\n" + "=" * 60)

    return "\n".join(lines)


def get_utilization_statistics(
    cases: List[Dict[str, Any]], resources: List[Dict[str, Any]]
) -> Dict[str, Any]:
    """Generate utilization statistics for export."""
    total_cases = len(cases)
    if total_cases == 0:
        return {
            "total_cases": 0,
            "status_distribution": {},
            "resource_utilization": {},
            "average_progress": 0,
            "completion_rate": 0,
            "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }

    # Status distribution
    status_counts = {}
    total_progress = 0

    for case in cases:
        status = case.get("status", "unknown")
        status_counts[status] = status_counts.get(status, 0) + 1
        total_progress += case.get("progress", 0)

    # Resource utilization
    resource_stats = {}
    for resource in resources:
        group = resource.get("pueue_group", "unknown")
        status = resource.get("status", "unknown")
        if group not in resource_stats:
            resource_stats[group] = {"available": 0, "assigned": 0}
        resource_stats[group][status] = resource_stats[group].get(status, 0) + 1

    # Calculate rates
    completed_cases = status_counts.get("completed", 0)
    completion_rate = (completed_cases / total_cases * 100) if total_cases > 0 else 0
    average_progress = total_progress / total_cases if total_cases > 0 else 0

    return {
        "total_cases": total_cases,
        "status_distribution": status_counts,
        "resource_utilization": resource_stats,
        "average_progress": round(average_progress, 2),
        "completion_rate": round(completion_rate, 2),
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }


def export_utilization_statistics(
    cases: List[Dict[str, Any]], resources: List[Dict[str, Any]], file_path: str
) -> None:
    """Export utilization statistics to JSON file."""
    stats = get_utilization_statistics(cases, resources)

    with open(file_path, "w", encoding="utf-8") as jsonfile:
        json.dump(stats, jsonfile, indent=2, ensure_ascii=False)


def show_interactive_menu(console: Console) -> str:
    """Show interactive menu and return user choice."""
    console.print("\n[bold cyan]Interactive Dashboard Menu[/bold cyan]")
    console.print("1. Filter cases")
    console.print("2. Search cases")
    console.print("3. Export data")
    console.print("4. Show utilization statistics")
    console.print("5. Back to live dashboard")
    console.print("q. Quit")

    choice = Prompt.ask("Select an option", choices=["1", "2", "3", "4", "5", "q"])
    return choice


def handle_filter_menu(console: Console) -> Optional[DashboardFilter]:
    """Handle filtering options and return filter object."""
    console.print("\n[bold yellow]Filter Options[/bold yellow]")

    # Status filter
    status = Prompt.ask(
        "Filter by status (optional)",
        choices=["", "submitted", "submitting", "running", "completed", "failed"],
        default="",
    )
    status_filter = status if status else None

    # GPU group filter
    gpu_group = Prompt.ask("Filter by GPU group (optional)", default="")
    gpu_group_filter = gpu_group if gpu_group else None

    # Search term
    search_term = Prompt.ask("Search term (optional)", default="")

    if not any([status_filter, gpu_group_filter, search_term]):
        return None

    return DashboardFilter(
        status_filter=status_filter,
        gpu_group_filter=gpu_group_filter,
        search_term=search_term,
    )


def handle_export_menu(
    console: Console,
    case_data: List[Dict[str, Any]],
    resource_data: List[Dict[str, Any]],
) -> None:
    """Handle export options."""
    console.print("\n[bold yellow]Export Options[/bold yellow]")
    console.print("1. Export cases to CSV")
    console.print("2. Export all data to JSON")
    console.print("3. Export utilization statistics")
    console.print("4. Export dashboard snapshot")

    choice = Prompt.ask("Select export type", choices=["1", "2", "3", "4"])

    if choice == "1":
        filename = Prompt.ask("CSV filename", default="cases_export.csv")
        export_to_csv(case_data, filename)
        console.print(f"[green]Cases exported to {filename}[/green]")

    elif choice == "2":
        filename = Prompt.ask("JSON filename", default="dashboard_export.json")
        export_to_json(case_data, resource_data, filename)
        console.print(f"[green]Data exported to {filename}[/green]")

    elif choice == "3":
        filename = Prompt.ask("Statistics filename", default="utilization_stats.json")
        export_utilization_statistics(case_data, resource_data, filename)
        console.print(f"[green]Statistics exported to {filename}[/green]")

    elif choice == "4":
        filename = Prompt.ask("Snapshot filename", default="dashboard_snapshot.txt")
        snapshot = format_dashboard_snapshot(case_data, resource_data)
        with open(filename, "w", encoding="utf-8") as f:
            f.write(snapshot)
        console.print(f"[green]Snapshot exported to {filename}[/green]")


def display_filtered_data(
    console: Console,
    case_data: List[Dict[str, Any]],
    resource_data: List[Dict[str, Any]],
    filter_obj: Optional[DashboardFilter] = None,
) -> None:
    """Display filtered data in a static view."""
    filtered_cases = case_data

    if filter_obj:
        # Apply filters
        filtered_cases = filter_cases(case_data, filter_obj)
        filtered_cases = search_cases(filtered_cases, filter_obj)

        # Show filter summary
        filter_summary = []
        if filter_obj.status_filter:
            filter_summary.append(f"Status: {filter_obj.status_filter}")
        if filter_obj.gpu_group_filter:
            filter_summary.append(f"GPU Group: {filter_obj.gpu_group_filter}")
        if filter_obj.search_term:
            filter_summary.append(f"Search: '{filter_obj.search_term}'")

        if filter_summary:
            console.print(
                f"\n[yellow]Active Filters: {', '.join(filter_summary)}[/yellow]"
            )
        console.print(
            f"[cyan]Showing {len(filtered_cases)} of {len(case_data)} cases[/cyan]"
        )

    # Display filtered data
    layout = create_tables(filtered_cases, resource_data)
    console.print(layout)


def create_tables(
    case_data: List[Dict[str, Any]], resource_data: List[Dict[str, Any]]
) -> Layout:
    """Creates the layout containing tables for cases and GPU resources."""
    layout = Layout()
    layout.split_row(
        Layout(name="cases_panel", ratio=2),  # Left panel for cases
        Layout(name="gpu_panel", ratio=1),    # Right panel for GPU resources
    )

    # --- Cases Table ---
    updated_time = datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S")
    case_table = Table(
        title=f"Live Case Status (Updated: {updated_time})",
        expand=True,
    )
    case_table.add_column("ID", justify="right", style="cyan", no_wrap=True)
    case_table.add_column("Case Path", style="magenta", max_width=50)
    case_table.add_column("Status", style="green")
    case_table.add_column("Progress", justify="right", style="yellow")
    case_table.add_column("Pueue Group", style="blue")
    case_table.add_column("Task ID", justify="right", style="dim")
    case_table.add_column("Submitted At", style="dim")
    case_table.add_column("Updated At", style="dim")

    for case in case_data:
        # Format progress with a percentage sign
        progress = f"{case['progress']}%"

        # Style status based on its value
        status = case["status"]
        if status == "failed":
            status_style = "[bold red]failed[/bold red]"
        elif status == "completed":
            status_style = "[bold green]completed[/bold green]"
        elif status == "running":
            status_style = "[yellow]running[/yellow]"
        else:
            status_style = f"[{status}]"

        task_id_str = (
            str(case["pueue_task_id"]) if case["pueue_task_id"] is not None else "N/A"
        )
        
        # Format timestamps to show only time (HH:MM:SS)
        def format_time_only(timestamp_str):
            try:
                if timestamp_str:
                    dt = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
                    return dt.strftime("%H:%M:%S")
                return "N/A"
            except:
                # Fallback: try to parse as "%Y-%m-%d %H:%M:%S" format
                try:
                    dt = datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S")
                    return dt.strftime("%H:%M:%S")
                except:
                    return "N/A"
        
        case_table.add_row(
            str(case["case_id"]),
            case["case_path"],
            status_style,
            progress,
            case["pueue_group"] or "N/A",
            task_id_str,
            format_time_only(case["submitted_at"]),
            format_time_only(case["status_updated_at"]),
        )

    # --- GPU Resources Table ---
    resource_table = Table(title="GPU Resource Status", expand=True, show_header=True, header_style="bold magenta")
    resource_table.add_column("GPU", style="blue", width=12)
    resource_table.add_column("Status", style="green", width=10)
    resource_table.add_column("Case ID", justify="right", style="cyan", width=8)

    for resource in resource_data:
        status = resource["status"]
        if status == "assigned":
            status_style = "[bold yellow]ASSIGNED[/bold yellow]"
        elif status == "busy":
            status_style = "[bold orange3]BUSY[/bold orange3]"
        elif status == "zombie":
            status_style = "[bold red]ZOMBIE[/bold red]"
        elif status == "available":
            status_style = "[green]AVAILABLE[/green]"
        else:
            status_style = f"[{status.upper()}]"  # Fallback for any other status

        # Shorten GPU group name for better display
        gpu_name = resource["pueue_group"]
        if gpu_name.startswith("gpu_"):
            gpu_name = gpu_name[4:].upper()  # Remove 'gpu_' prefix and capitalize

        case_id_display = (
            str(resource["assigned_case_id"])
            if resource["assigned_case_id"] is not None
            else "-"
        )

        resource_table.add_row(
            gpu_name,
            status_style,
            case_id_display,
        )

    layout["cases_panel"].update(
        Panel(Align.center(case_table, vertical="middle"), title="Case Status")
    )
    layout["gpu_panel"].update(
        Panel(Align.center(resource_table, vertical="middle"), title="GPU Resources")
    )

    return layout


def display_dashboard(auto_refresh: bool = True, interactive: bool = False) -> None:
    """
    Displays a live-updating dashboard with the status of cases and resources.
    If auto_refresh is True, the dashboard will update every 2 seconds.
    If auto_refresh is False, it will display once and exit.
    If interactive is True, provides menu-driven interface for filtering and export.
    """
    logger.info(
        "Dashboard initialization started",
        context=LogContext(extra_data={
            "category": "dashboard_startup",
            "operation": "display_dashboard",
            "auto_refresh": auto_refresh,
            "interactive_mode": interactive
        })
    )
    console = Console()
    db_manager = None

    try:
        # Load config to find the database
        logger.info(
            "Loading dashboard configuration",
            context=LogContext(extra_data={
                "category": "dashboard_config",
                "operation": "load_config",
                "config_path": CONFIG_PATH
            })
        )
        config_manager = ConfigManager(config_path=CONFIG_PATH)
        db_path = config_manager.get("database.path")
        logger.info(
            "Configuration loaded successfully",
            context=LogContext(extra_data={
                "category": "dashboard_config",
                "operation": "load_config",
                "status": "success"
            })
        )

        logger.debug(
            "Database path from config",
            # extra=LogContext.create(
                category="dashboard_config",
                operation="resolve_db_path",
                raw_db_path=db_path
            # )
        )
        
        # Resolve path relative to project root if it's not absolute
        if not os.path.isabs(db_path):
            db_path = os.path.join(PROJECT_ROOT, db_path)
        logger.info(
            "Database path resolved",
            # extra=LogContext.create(
                category="dashboard_config",
                operation="resolve_db_path",
                resolved_db_path=db_path
            # )
        )
        
        if not db_path:
            error_msg = f"Database path not found in '{CONFIG_PATH}'"
            logger.error(
                "Database path not configured",
                # extra=LogContext.create(
                    category="dashboard_config_error",
                    operation="validate_db_path",
                    config_path=CONFIG_PATH,
                    error_reason="missing_db_path"
                # )
            )
            console.print(f"[bold red]Error: {error_msg}. Check config validation.[/bold red]")
            return

        if not Path(db_path).exists():
            error_msg = f"Database file not found at '{db_path}'"
            logger.warning(
                "Database file does not exist",
                # extra=LogContext.create(
                    category="dashboard_warning",
                    operation="check_db_exists",
                    db_path=db_path,
                    error_reason="file_not_found"
                # )
            )
            console.print(f"[bold yellow]Database file not found at '{db_path}'.[/bold yellow]")
            console.print("Please run the main application first to create the database.")
            # Create dummy tables to show the structure
            layout = create_tables([], [])
            console.print(layout)
            return

        logger.info(
            "Connecting to database",
            # extra=LogContext.create(
                category="dashboard_database",
                operation="connect_db",
                db_path=db_path
            # )
        )
        db_manager = DatabaseManager(db_path=db_path)
        logger.info(
            "Database connection established",
            # extra=LogContext.create(
                category="dashboard_database",
                operation="connect_db",
                status="success"
            # )
        )
        
        console.print("[bold cyan]MQI Communicator Dashboard[/bold cyan]")
        console.print(f"Connected to database: [yellow]{db_path}[/yellow]")

        if auto_refresh:
            console.print("Press [bold]Ctrl+C[/bold] to exit.")
            logger.info(
                "Dashboard mode configured",
                # extra=LogContext.create(
                    category="dashboard_mode",
                    operation="configure_mode",
                    auto_refresh_enabled=True
                # )
            )

        # Create initial tables
        logger.info(
            "Loading initial data from database",
            # extra=LogContext.create(
                category="dashboard_data",
                operation="initial_load"
            # )
        )
        all_cases = db_manager.cursor.execute(
            "SELECT * FROM cases ORDER BY case_id DESC"
        ).fetchall()
        
        all_resources = db_manager.cursor.execute(
            "SELECT * FROM gpu_resources ORDER BY pueue_group"
        ).fetchall()
        
        logger.info(
            "Initial data loaded successfully",
            context=LogContext(
                extra_data={
                    "category": "dashboard_data",
                    "operation": "initial_load",
                    "cases_count": len(all_cases),
                    "resources_count": len(all_resources)
                }
            )
        )

        # Convert rows to dictionaries for easier processing
        case_data = [dict(row) for row in all_cases]
        resource_data = [dict(row) for row in all_resources]
        
        logger.debug(
            "Data converted to dictionaries",
            # extra=LogContext.create(
                category="dashboard_data",
                operation="convert_data",
                status="success"
            # )
        )

        logger.info(
            "Displaying initial dashboard layout",
            # extra=LogContext.create(
                category="dashboard_display",
                operation="show_initial_layout"
            # )
        )
        layout = create_tables(case_data, resource_data)
        console.print(layout)

        if interactive:
            # Interactive mode
            while True:
                try:
                    choice = show_interactive_menu(console)

                    if choice == "q":
                        break
                    elif choice == "1" or choice == "2":  # Filter or search
                        filter_obj = handle_filter_menu(console)
                        if filter_obj:
                            # Refresh data and apply filters
                            all_cases = db_manager.cursor.execute(
                                "SELECT * FROM cases ORDER BY case_id DESC"
                            ).fetchall()
                            all_resources = db_manager.cursor.execute(
                                "SELECT * FROM gpu_resources ORDER BY pueue_group"
                            ).fetchall()
                            case_data = [dict(row) for row in all_cases]
                            resource_data = [dict(row) for row in all_resources]

                            display_filtered_data(
                                console, case_data, resource_data, filter_obj
                            )
                        else:
                            console.print("[yellow]No filters applied[/yellow]")
                    elif choice == "3":  # Export
                        # Refresh data before export
                        all_cases = db_manager.cursor.execute(
                            "SELECT * FROM cases ORDER BY case_id DESC"
                        ).fetchall()
                        all_resources = db_manager.cursor.execute(
                            "SELECT * FROM gpu_resources ORDER BY pueue_group"
                        ).fetchall()
                        case_data = [dict(row) for row in all_cases]
                        resource_data = [dict(row) for row in all_resources]

                        handle_export_menu(console, case_data, resource_data)
                    elif choice == "4":  # Show statistics
                        # Refresh data and show statistics
                        all_cases = db_manager.cursor.execute(
                            "SELECT * FROM cases ORDER BY case_id DESC"
                        ).fetchall()
                        all_resources = db_manager.cursor.execute(
                            "SELECT * FROM gpu_resources ORDER BY pueue_group"
                        ).fetchall()
                        case_data = [dict(row) for row in all_cases]
                        resource_data = [dict(row) for row in all_resources]

                        stats = get_utilization_statistics(case_data, resource_data)
                        console.print("\n[bold cyan]Utilization Statistics[/bold cyan]")
                        console.print(f"Total Cases: {stats['total_cases']}")
                        console.print(f"Average Progress: {stats['average_progress']}%")
                        console.print(f"Completion Rate: {stats['completion_rate']}%")
                        console.print("\nStatus Distribution:")
                        for status, count in stats["status_distribution"].items():
                            console.print(f"  {status.capitalize()}: {count}")
                        console.print("\nResource Utilization:")
                        for group, util in stats["resource_utilization"].items():
                            available = util.get("available", 0)
                            assigned = util.get("assigned", 0)
                            console.print(
                                f"  {group}: {assigned} assigned, {available} available"
                            )
                    elif choice == "5":  # Back to live dashboard
                        console.print("\n[cyan]Returning to live dashboard...[/cyan]")
                        break

                    if choice != "5":
                        console.input("\n[dim]Press Enter to continue...[/dim]")

                except KeyboardInterrupt:
                    break

        if auto_refresh and not interactive:
            logger.info(
                "Starting auto-refresh dashboard",
                # extra=LogContext.create(
                    category="dashboard_refresh",
                    operation="start_auto_refresh",
                    refresh_rate=0.5
                # )
            )
            refresh_count = 0
            try:
                with Live(layout, refresh_per_second=0.5, redirect_stderr=False) as live:
                    logger.info(
                        "Live dashboard started successfully",
                        # extra=LogContext.create(
                            category="dashboard_refresh",
                            operation="start_live_display",
                            status="success"
                        # )
                    )
                    while True:
                        refresh_count += 1
                        
                        try:
                            # Fetch all cases and resources
                            all_cases = db_manager.cursor.execute(
                                "SELECT * FROM cases ORDER BY case_id DESC"
                            ).fetchall()
                            all_resources = db_manager.cursor.execute(
                                "SELECT * FROM gpu_resources ORDER BY pueue_group"
                            ).fetchall()

                            # Convert rows to dictionaries for easier processing
                            case_data = [dict(row) for row in all_cases]
                            resource_data = [dict(row) for row in all_resources]

                            # Create new layout with updated data
                            updated_layout = create_tables(case_data, resource_data)
                            
                            # Update the live display
                            live.update(updated_layout)
                            
                            # Log every 30th refresh to track activity without spam
                            if refresh_count % 30 == 0:
                                logger.info(
                                    "Dashboard refresh status update",
                                    # extra=LogContext.create(
                                        category="dashboard_refresh",
                                        operation="refresh_status",
                                        refresh_count=refresh_count,
                                        cases_count=len(case_data),
                                        resources_count=len(resource_data)
                                    # )
                                )
                            
                        except Exception as db_error:
                            logger.error_with_exception(
                                "Error during dashboard refresh cycle",
                                db_error,
                                # extra=LogContext.create(
                                    category="dashboard_refresh_error",
                                    operation="refresh_cycle",
                                    refresh_count=refresh_count
                                # )
                            )
                        time.sleep(2)  # Refresh interval
                        
            except Exception as live_error:
                logger.error_with_exception(
                    "Error with Rich Live display",
                    live_error,
                    # extra=LogContext.create(
                        category="dashboard_live_error",
                        operation="live_display"
                    # )
                )
                raise

    except FileNotFoundError as e:
        error_msg = f"Config file not found at '{CONFIG_PATH}'"
        logger.error_with_exception(
            "Configuration file not found",
            e,
            # extra=LogContext.create(
                category="dashboard_config_error",
                operation="load_config",
                config_path=CONFIG_PATH
            # )
        )
        console.print(f"[bold red]Error: {error_msg}[/bold red]")

    except ConfigValidationError as e:
        error_msg = f"Configuration error in '{CONFIG_PATH}': {e}"
        logger.error_with_exception(
            "Configuration validation failed",
            e,
            # extra=LogContext.create(
                category="dashboard_config_error",
                operation="validate_config",
                config_path=CONFIG_PATH
            # )
        )
        console.print(f"[bold red]Error: {error_msg}[/bold red]")
        
    except KeyboardInterrupt:
        logger.info(
            "Dashboard shutdown requested by user",
            # extra=LogContext.create(
                category="dashboard_shutdown",
                operation="keyboard_interrupt",
                reason="user_requested"
            # )
        )
        console.print("\n[bold cyan]Dashboard closed.[/bold cyan]")
        
    except Exception as e:
        logger.error_with_exception(
            "Unexpected error in dashboard",
            e,
            # extra=LogContext.create(
                category="dashboard_error",
                operation="display_dashboard",
                error_type=type(e).__name__
            # )
        )
        console.print(f"\n[bold red]An unexpected error occurred: {e}[/bold red]")
        
    finally:
        if db_manager:
            logger.info(
                "Closing database connection",
                # extra=LogContext.create(
                    category="dashboard_shutdown",
                    operation="close_db_connection"
                # )
            )
            db_manager.close()
            logger.info(
                "Database connection closed successfully",
                # extra=LogContext.create(
                    category="dashboard_shutdown",
                    operation="close_db_connection",
                    status="success"
                # )
            )


if __name__ == "__main__":
    display_dashboard(auto_refresh=True)

=True)
