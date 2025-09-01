"""
Process-safe DB interface.
Manages database interactions with proper concurrency handling.
"""
import sqlite3
import threading
from pathlib import Path
from datetime import datetime, timezone
from contextlib import contextmanager
from typing import Dict, Any, Optional, List

class DatabaseHandler:
    """
    Handler for database interactions with a process-safe design.
    Each worker process should instantiate its own DatabaseHandler. This ensures that
    each process has its own database connection, preventing conflicts over shared
    connection objects. SQLite in WAL mode can handle concurrent writes from
    multiple processes safely.
    """

    def __init__(self, db_path: str):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self.conn = self._create_optimized_connection()
        self.init_db()

    def _create_optimized_connection(self) -> sqlite3.Connection:
        """Create an optimized SQLite connection."""
        conn = sqlite3.connect(str(self.db_path), check_same_thread=False, timeout=10)
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA foreign_keys = ON;")
        conn.row_factory = sqlite3.Row
        return conn

    @contextmanager
    def transaction(self):
        """Context manager for database transactions with a thread lock."""
        with self._lock:
            try:
                yield self.conn
                self.conn.commit()
            except Exception:
                self.conn.rollback()
                raise

    def init_db(self):
        """Initialize the database with the required tables and indexes."""
        with self.transaction() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS cases (
                    case_id TEXT PRIMARY KEY,
                    case_path TEXT NOT NULL UNIQUE,
                    status TEXT NOT NULL,
                    progress INTEGER NOT NULL DEFAULT 0,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS workflow_steps (
                    step_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    case_id TEXT NOT NULL,
                    step_name TEXT NOT NULL,
                    status TEXT NOT NULL,
                    started_at TEXT,
                    completed_at TEXT,
                    error_message TEXT,
                    FOREIGN KEY (case_id) REFERENCES cases (case_id) ON DELETE CASCADE
                )
            """)
            conn.execute("CREATE INDEX IF NOT EXISTS idx_cases_status ON cases (status)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_workflow_steps_case_id ON workflow_steps (case_id)")

    def add_case(self, case_id: str, case_path: str, status: str = "NEW"):
        now = datetime.now(timezone.utc).isoformat()
        with self.transaction() as conn:
            conn.execute(
                "INSERT INTO cases (case_id, case_path, status, created_at, updated_at) VALUES (?, ?, ?, ?, ?)",
                (case_id, case_path, status, now, now)
            )

    def update_case_status(self, case_id: str, status: str, progress: Optional[int] = None):
        now = datetime.now(timezone.utc).isoformat()
        with self.transaction() as conn:
            if progress is not None:
                conn.execute(
                    "UPDATE cases SET status = ?, progress = ?, updated_at = ? WHERE case_id = ?",
                    (status, progress, now, case_id)
                )
            else:
                conn.execute(
                    "UPDATE cases SET status = ?, updated_at = ? WHERE case_id = ?",
                    (status, now, case_id)
                )

    def get_case(self, case_id: str) -> Optional[Dict[str, Any]]:
        with self._lock:
            cursor = self.conn.cursor()
            cursor.execute("SELECT * FROM cases WHERE case_id = ?", (case_id,))
            row = cursor.fetchone()
        return dict(row) if row else None

    def get_cases_by_status(self, status: str) -> List[Dict[str, Any]]:
        with self._lock:
            cursor = self.conn.cursor()
            cursor.execute("SELECT * FROM cases WHERE status = ? ORDER BY created_at", (status,))
            rows = cursor.fetchall()
        return [dict(row) for row in rows]

    def record_workflow_step(self, case_id: str, step_name: str, status: str, error_message: Optional[str] = None):
        now = datetime.now(timezone.utc).isoformat()
        with self.transaction() as conn:
            if status == 'STARTED':
                conn.execute(
                    """
                    INSERT INTO workflow_steps (case_id, step_name, status, started_at)
                    VALUES (?, ?, ?, ?)
                    """,
                    (case_id, step_name, status, now)
                )
            else: # COMPLETED, FAILED
                conn.execute(
                    """
                    UPDATE workflow_steps
                    SET status = ?, completed_at = ?, error_message = ?
                    WHERE case_id = ? AND step_name = ? AND completed_at IS NULL
                    """,
                    (status, now, error_message, case_id, step_name)
                )

    def get_workflow_steps(self, case_id: str) -> List[Dict[str, Any]]:
        with self._lock:
            cursor = self.conn.cursor()
            cursor.execute("SELECT * FROM workflow_steps WHERE case_id = ? ORDER BY started_at", (case_id,))
            rows = cursor.fetchall()
        return [dict(row) for row in rows]

    def close(self):
        """Close the database connection."""
        if self.conn:
            self.conn.close()