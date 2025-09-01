import sqlite3
import os
import threading
import time
from pathlib import Path
from datetime import datetime, timezone, timedelta
from typing import Optional, Dict, Any, List
from dataclasses import dataclass
from contextlib import contextmanager
from collections import OrderedDict


# Define Korea Standard Time (KST) as UTC+9
KST = timezone(timedelta(hours=9))


@dataclass
class QueryPerformanceMetrics:
    """Performance metrics for database queries."""

    query_count: int = 0
    total_execution_time: float = 0.0
    cache_hits: int = 0
    cache_misses: int = 0
    slow_queries: int = 0
    average_execution_time: float = 0.0

    def add_query(
        self,
        execution_time: float,
        was_cached: bool = False,
        slow_threshold: float = 0.05,
    ) -> None:
        """Record a query execution."""
        self.query_count += 1
        self.total_execution_time += execution_time
        self.average_execution_time = self.total_execution_time / self.query_count

        if was_cached:
            self.cache_hits += 1
        else:
            self.cache_misses += 1

        if execution_time > slow_threshold:
            self.slow_queries += 1

    def get_cache_hit_rate(self) -> float:
        """Calculate cache hit rate percentage."""
        total_requests = self.cache_hits + self.cache_misses
        if total_requests == 0:
            return 0.0
        return (self.cache_hits / total_requests) * 100


class QueryCache:
    """Simple LRU cache for database query results."""

    def __init__(self, max_size: int = 1000, ttl_seconds: int = 300):
        """
        Initialize query cache.

        Args:
            max_size: Maximum number of cached items
            ttl_seconds: Time-to-live for cache entries in seconds
        """
        self.max_size = max_size
        self.ttl_seconds = ttl_seconds
        self._cache: OrderedDict = OrderedDict()
        self._timestamps: Dict[str, float] = {}
        self._lock = threading.Lock()

    def get(self, key: str) -> Optional[Any]:
        """Get item from cache if it exists and is not expired."""
        with self._lock:
            if key not in self._cache:
                return None

            # Check if expired
            if time.time() - self._timestamps[key] > self.ttl_seconds:
                self._cache.pop(key)
                self._timestamps.pop(key)
                return None

            # Move to end (most recently used)
            value = self._cache.pop(key)
            self._cache[key] = value
            return value

    def put(self, key: str, value: Any) -> None:
        """Put item in cache, evicting oldest if necessary."""
        with self._lock:
            # Remove if already exists
            if key in self._cache:
                self._cache.pop(key)

            # Add new item
            self._cache[key] = value
            self._timestamps[key] = time.time()

            # Evict oldest items if over capacity
            while len(self._cache) > self.max_size:
                oldest_key = next(iter(self._cache))
                self._cache.pop(oldest_key)
                self._timestamps.pop(oldest_key)

    def invalidate(self, pattern: str = None) -> None:
        """Invalidate cache entries matching pattern (or all if None)."""
        with self._lock:
            if pattern is None:
                self._cache.clear()
                self._timestamps.clear()
            else:
                # Remove keys that contain the pattern
                keys_to_remove = [k for k in self._cache.keys() if pattern in k]
                for key in keys_to_remove:
                    self._cache.pop(key, None)
                    self._timestamps.pop(key, None)

    def size(self) -> int:
        """Get current cache size."""
        with self._lock:
            return len(self._cache)


class DatabaseManager:
    """
    Enhanced database manager with performance optimizations including indexing,
    caching, and connection pooling for sub-50ms query performance.
    """

    def __init__(
        self,
        db_path: Optional[str] = None,
        config: Optional[Dict[str, Any]] = None,
    ) -> None:
        """
        Initialize the optimized database manager.

        Args:
            db_path: The path to the SQLite database file
            config: The application's configuration dictionary

        Raises:
            ValueError: If neither db_path nor config is provided
        """
        if db_path:
            self.db_path = db_path
            # Use hardcoded defaults when db_path is provided directly (e.g., for tests)
            enable_cache = True
            cache_size = 1000
            cache_ttl = 300
            self.enable_wal_mode = True
            self.connection_timeout = 30
        elif config:
            db_config = config.get("database", {})
            self.db_path = db_config.get("path")
            if not self.db_path:
                raise ValueError("Database 'path' not found in configuration.")
            enable_cache = db_config.get("enable_cache", True)
            cache_size = db_config.get("cache_size", 1000)
            cache_ttl = db_config.get("cache_ttl_seconds", 300)
            self.enable_wal_mode = db_config.get("enable_wal_mode", True)
            self.connection_timeout = db_config.get("connection_timeout_seconds", 30)
        else:
            raise ValueError("Either db_path or config must be provided.")

        # Ensure the directory for the database file exists
        db_dir = Path(self.db_path).parent
        os.makedirs(db_dir, exist_ok=True)

        # Initialize connection with optimizations
        self.conn = self._create_optimized_connection()

        # Use Row factory to allow accessing columns by name
        self.conn.row_factory = sqlite3.Row
        self.cursor = self.conn.cursor()

        # Initialize caching system
        self.enable_cache = enable_cache
        if self.enable_cache:
            self.query_cache = QueryCache(max_size=cache_size, ttl_seconds=cache_ttl)

        # Performance metrics
        self.metrics = QueryPerformanceMetrics()
        self._lock = threading.Lock()

    def _create_optimized_connection(self) -> sqlite3.Connection:
        """Create an optimized SQLite connection."""
        conn = sqlite3.connect(
            self.db_path, check_same_thread=False, timeout=self.connection_timeout
        )

        # Enable WAL mode for better concurrency
        if self.enable_wal_mode:
            conn.execute("PRAGMA journal_mode = WAL")

        # Performance optimizations
        conn.execute("PRAGMA synchronous = NORMAL")  # Faster than FULL, safer than OFF
        conn.execute("PRAGMA cache_size = -64000")  # 64MB cache
        conn.execute("PRAGMA temp_store = MEMORY")  # Store temp tables in memory
        conn.execute("PRAGMA mmap_size = 268435456")  # 256MB memory map

        # Enable foreign key constraints
        conn.execute("PRAGMA foreign_keys = ON")

        return conn

    def _execute_with_metrics(
        self, query: str, params: tuple = (), cache_key: str = None
    ) -> List[sqlite3.Row]:
        """
        Execute query with performance metrics tracking and optional caching.

        Args:
            query: SQL query string
            params: Query parameters
            cache_key: Optional cache key for result caching

        Returns:
            Query results as list of sqlite3.Row objects
        """
        start_time = time.time()

        # Check cache first if enabled and cache_key provided
        if self.enable_cache and cache_key:
            cached_result = self.query_cache.get(cache_key)
            if cached_result is not None:
                execution_time = time.time() - start_time
                self.metrics.add_query(execution_time, was_cached=True)
                return cached_result

        # Execute query with thread-safe cursor
        with self._lock:
            cursor = self.conn.cursor()
            cursor.execute(query, params)
            results = cursor.fetchall()
            cursor.close()

        # Cache results if caching is enabled and cache_key provided
        if self.enable_cache and cache_key:
            # Convert to list for caching (sqlite3.Row objects are not thread-safe)
            cached_results = [dict(row) for row in results]
            self.query_cache.put(cache_key, cached_results)

        execution_time = time.time() - start_time
        self.metrics.add_query(execution_time, was_cached=False)

        return results

    def _create_tables(self) -> None:
        """Create necessary tables with optimized indexes."""
        # Create cases table
        self.cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS cases (
                case_id INTEGER PRIMARY KEY AUTOINCREMENT,
                case_path TEXT NOT NULL UNIQUE,
                status TEXT NOT NULL,
                progress INTEGER NOT NULL,
                priority INTEGER DEFAULT 2,
                pueue_group TEXT,
                pueue_task_id INTEGER,
                submitted_at TEXT NOT NULL,
                completed_at TEXT,
                status_updated_at TEXT NOT NULL,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

        # Create gpu_resources table
        self.cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS gpu_resources (
                pueue_group TEXT PRIMARY KEY,
                status TEXT NOT NULL,
                assigned_case_id INTEGER,
                last_updated TEXT DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (assigned_case_id) REFERENCES cases (case_id)
            )
            """
        )

        # Create performance-critical indexes
        self._create_indexes()
        self.conn.commit()

    def _create_indexes(self) -> None:
        """Create indexes for optimal query performance."""
        indexes = [
            # Cases table indexes
            "CREATE INDEX IF NOT EXISTS idx_cases_status ON cases (status)",
            "CREATE INDEX IF NOT EXISTS idx_cases_status_priority ON cases (status, priority DESC, created_at ASC)",
            "CREATE INDEX IF NOT EXISTS idx_cases_pueue_group ON cases (pueue_group)",
            "CREATE INDEX IF NOT EXISTS idx_cases_pueue_task_id ON cases (pueue_task_id)",
            "CREATE INDEX IF NOT EXISTS idx_cases_status_updated ON cases (status_updated_at)",
            "CREATE INDEX IF NOT EXISTS idx_cases_priority ON cases (priority)",
            "CREATE INDEX IF NOT EXISTS idx_cases_created_at ON cases (created_at)",
            # GPU resources table indexes
            "CREATE INDEX IF NOT EXISTS idx_gpu_resources_status ON gpu_resources (status)",
            "CREATE INDEX IF NOT EXISTS idx_gpu_resources_assigned_case ON gpu_resources (assigned_case_id)",
            "CREATE INDEX IF NOT EXISTS idx_gpu_resources_status_group ON gpu_resources (status, pueue_group)",
            # Composite indexes for common query patterns
            "CREATE INDEX IF NOT EXISTS idx_cases_status_created ON cases (status, created_at ASC)",
            "CREATE INDEX IF NOT EXISTS idx_cases_status_updated_at ON cases (status, status_updated_at DESC)",
        ]

        for index_sql in indexes:
            try:
                self.cursor.execute(index_sql)
            except sqlite3.Error as e:
                # Index might already exist, continue with others
                pass

    def init_db(self) -> None:
        """Initialize the database with tables and indexes."""
        self._migrate_schema()
        self._create_tables()

    def _migrate_schema(self) -> None:
        """
        Alters existing tables to add new columns if they are missing.
        This ensures backward compatibility with older database schemas.
        """
        try:
            with self.transaction():
                # Check and add 'priority' and 'created_at' to 'cases' table
                self.cursor.execute("PRAGMA table_info(cases)")
                case_columns = [col["name"] for col in self.cursor.fetchall()]

                if "priority" not in case_columns:
                    self.cursor.execute(
                        "ALTER TABLE cases ADD COLUMN priority INTEGER DEFAULT 2"
                    )
                if "created_at" not in case_columns:
                    # SQLite doesn't support CURRENT_TIMESTAMP in ALTER TABLE ADD COLUMN
                    # Add column with NULL default first
                    self.cursor.execute(
                        "ALTER TABLE cases ADD COLUMN created_at TEXT"
                    )
                    # Update existing rows to use their submitted_at time
                    current_time = datetime.now(KST).isoformat()
                    self.cursor.execute(
                        """
                        UPDATE cases 
                        SET created_at = COALESCE(submitted_at, ?) 
                        WHERE created_at IS NULL
                        """,
                        (current_time,)
                    )

                # Check and add 'last_updated' to 'gpu_resources' table
                self.cursor.execute("PRAGMA table_info(gpu_resources)")
                gpu_columns = [col["name"] for col in self.cursor.fetchall()]
                if "last_updated" not in gpu_columns:
                    # SQLite doesn't support CURRENT_TIMESTAMP in ALTER TABLE ADD COLUMN
                    # Add column with NULL default first
                    self.cursor.execute(
                        "ALTER TABLE gpu_resources ADD COLUMN last_updated TEXT"
                    )
                    # Update existing rows with current timestamp
                    current_time = datetime.now(KST).isoformat()
                    self.cursor.execute(
                        """
                        UPDATE gpu_resources 
                        SET last_updated = ? 
                        WHERE last_updated IS NULL
                        """,
                        (current_time,)
                    )
        except sqlite3.Error:
            # This can happen if the tables don't exist yet, which is fine.
            pass

    @contextmanager
    def transaction(self):
        """Context manager for database transactions."""
        with self._lock:
            try:
                self.conn.execute("BEGIN IMMEDIATE")
                yield
                self.conn.commit()
            except Exception:
                self.conn.rollback()
                raise

    # Optimized query methods

    def add_case(self, case_path: str, priority: int = 2) -> Optional[int]:
        """Add a new case with optional priority."""
        now_iso = datetime.now(KST).isoformat()

        with self._lock:
            cursor = self.conn.cursor()
            cursor.execute(
                """
                INSERT INTO cases
                (case_path, status, progress, priority, submitted_at, status_updated_at, created_at)
                VALUES (?, 'submitted', 0, ?, ?, ?, ?)
                """,
                (case_path, priority, now_iso, now_iso, now_iso),
            )
            case_id = cursor.lastrowid
            self.conn.commit()
            cursor.close()

        # Invalidate relevant cache entries
        if self.enable_cache:
            self.query_cache.invalidate("cases_by_status")

        return case_id

    def get_case_by_id(self, case_id: int) -> Optional[Dict[str, Any]]:
        """Get case by ID with caching."""
        cache_key = f"case_by_id_{case_id}" if self.enable_cache else None

        results = self._execute_with_metrics(
            "SELECT * FROM cases WHERE case_id = ?", (case_id,), cache_key=cache_key
        )

        if results:
            return dict(results[0]) if not self.enable_cache else results[0]
        return None

    def get_cases_by_status(
        self, status: str, limit: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """Get cases by status with optimized indexing and optional caching."""
        query = "SELECT * FROM cases WHERE status = ? ORDER BY priority DESC, created_at ASC"
        params = [status]

        if limit:
            query += " LIMIT ?"
            params.append(limit)

        cache_key = (
            f"cases_by_status_{status}_{limit}"
            if self.enable_cache and limit and limit <= 50
            else None
        )

        results = self._execute_with_metrics(query, tuple(params), cache_key=cache_key)

        if self.enable_cache and cache_key:
            return results  # Already converted to dict in caching
        else:
            return [dict(row) for row in results]

    def get_cases_by_priority_and_status(
        self, status: str, min_priority: int = 1, limit: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """Get cases by status and minimum priority with optimized query."""
        query = """
        SELECT * FROM cases 
        WHERE status = ? AND priority >= ? 
        ORDER BY priority DESC, created_at ASC
        """
        params = [status, min_priority]

        if limit:
            query += " LIMIT ?"
            params.append(limit)

        results = self._execute_with_metrics(query, tuple(params))
        return [dict(row) for row in results]

    def update_case_status(self, case_id: int, status: str, progress: int) -> None:
        """Update case status with cache invalidation."""
        now_iso = datetime.now(KST).isoformat()

        with self.transaction():
            self.cursor.execute(
                """
                UPDATE cases
                SET status = ?, progress = ?, status_updated_at = ?
                WHERE case_id = ?
                """,
                (status, progress, now_iso, case_id),
            )

        # Invalidate relevant cache entries
        if self.enable_cache:
            self.query_cache.invalidate("cases_by_status")
            self.query_cache.invalidate(f"case_by_id_{case_id}")

    def find_and_lock_any_available_gpu(self, case_id: int) -> Optional[str]:
        """Atomically find and lock available GPU with optimized query."""
        with self.conn:
            # Use index-optimized query for better performance
            self.cursor.execute(
                """
                UPDATE gpu_resources
                SET status = 'assigned', assigned_case_id = ?, last_updated = CURRENT_TIMESTAMP
                WHERE pueue_group = (
                    SELECT pueue_group FROM gpu_resources
                    WHERE status = 'available'
                    ORDER BY pueue_group
                    LIMIT 1
                )
                """,
                (case_id,),
            )

            if self.cursor.rowcount > 0:
                self.cursor.execute(
                    "SELECT pueue_group FROM gpu_resources WHERE assigned_case_id = ?",
                    (case_id,),
                )
                resource = self.cursor.fetchone()
                if resource:
                    # Invalidate GPU resource cache
                    if self.enable_cache:
                        self.query_cache.invalidate("gpu_resources")
                    return resource["pueue_group"]
        return None

    def get_performance_metrics(self) -> Dict[str, Any]:
        """Get database performance metrics."""
        cache_stats = {}
        if self.enable_cache:
            cache_stats = {
                "cache_size": self.query_cache.size(),
                "cache_hit_rate_percent": round(self.metrics.get_cache_hit_rate(), 2),
                "cache_hits": self.metrics.cache_hits,
                "cache_misses": self.metrics.cache_misses,
            }

        return {
            "query_count": self.metrics.query_count,
            "average_execution_time_ms": round(
                self.metrics.average_execution_time * 1000, 2
            ),
            "total_execution_time_ms": round(
                self.metrics.total_execution_time * 1000, 2
            ),
            "slow_queries": self.metrics.slow_queries,
            "cache_enabled": self.enable_cache,
            **cache_stats,
        }

    def optimize_database(self) -> None:
        """Run database optimization commands."""
        optimization_commands = ["PRAGMA optimize", "VACUUM", "REINDEX", "ANALYZE"]

        for command in optimization_commands:
            try:
                start_time = time.time()
                self.cursor.execute(command)
                self.conn.commit()
                execution_time = time.time() - start_time
                self.metrics.add_query(execution_time, was_cached=False)
            except sqlite3.Error as e:
                # Some commands might not be applicable, continue with others
                pass

    def reset_metrics(self) -> None:
        """Reset performance metrics."""
        self.metrics = QueryPerformanceMetrics()
        if self.enable_cache:
            self.query_cache.invalidate()

    # Delegate other methods to maintain compatibility
    def get_case_by_path(self, case_path: str) -> Optional[Dict[str, Any]]:
        """Get case by path."""
        results = self._execute_with_metrics(
            "SELECT * FROM cases WHERE case_path = ?", (case_path,)
        )
        return dict(results[0]) if results else None

    def update_case_pueue_task_id(self, case_id: int, pueue_task_id: int) -> None:
        """Update case Pueue task ID."""
        with self.transaction():
            self.cursor.execute(
                "UPDATE cases SET pueue_task_id = ? WHERE case_id = ?",
                (pueue_task_id, case_id),
            )

        if self.enable_cache:
            self.query_cache.invalidate(f"case_by_id_{case_id}")

    def update_case_pueue_group(self, case_id: int, pueue_group: str) -> None:
        """Update case Pueue group."""
        with self.transaction():
            self.cursor.execute(
                "UPDATE cases SET pueue_group = ? WHERE case_id = ?",
                (pueue_group, case_id),
            )

        if self.enable_cache:
            self.query_cache.invalidate(f"case_by_id_{case_id}")

    def update_case_completion(self, case_id: int, status: str) -> None:
        """Mark case as completed or failed."""
        completion_time = datetime.now(KST).isoformat()

        with self.transaction():
            self.cursor.execute(
                """
                UPDATE cases
                SET status = ?, progress = 100, completed_at = ?, status_updated_at = ?
                WHERE case_id = ?
                """,
                (status, completion_time, completion_time, case_id),
            )

        if self.enable_cache:
            self.query_cache.invalidate("cases_by_status")
            self.query_cache.invalidate(f"case_by_id_{case_id}")

    def release_gpu_resource(self, case_id: int) -> None:
        """Release GPU resource assigned to case."""
        with self.transaction():
            self.cursor.execute(
                """
                UPDATE gpu_resources
                SET status = 'available', assigned_case_id = NULL, last_updated = CURRENT_TIMESTAMP
                WHERE assigned_case_id = ?
                """,
                (case_id,),
            )

        if self.enable_cache:
            self.query_cache.invalidate("gpu_resources")

    def ensure_gpu_resource_exists(self, pueue_group: str) -> None:
        """Ensure GPU resource exists."""
        self.cursor.execute(
            "SELECT 1 FROM gpu_resources WHERE pueue_group = ?", (pueue_group,)
        )
        if self.cursor.fetchone() is None:
            with self.transaction():
                self.cursor.execute(
                    "INSERT INTO gpu_resources (pueue_group, status, last_updated) VALUES (?, ?, CURRENT_TIMESTAMP)",
                    (pueue_group, "available"),
                )

    def get_gpu_resource_by_case_id(self, case_id: int) -> Optional[Dict[str, Any]]:
        """Get GPU resource by assigned case ID."""
        results = self._execute_with_metrics(
            "SELECT * FROM gpu_resources WHERE assigned_case_id = ?", (case_id,)
        )
        return dict(results[0]) if results else None

    def get_gpu_resource(self, pueue_group: str) -> Optional[Dict[str, Any]]:
        """Get GPU resource by pueue group name."""
        results = self._execute_with_metrics(
            "SELECT * FROM gpu_resources WHERE pueue_group = ?", (pueue_group,)
        )
        return dict(results[0]) if results else None

    def get_resources_by_status(self, status: str) -> List[Dict[str, Any]]:
        """Get GPU resources by status."""
        results = self._execute_with_metrics(
            "SELECT * FROM gpu_resources WHERE status = ?", (status,)
        )
        return [dict(row) for row in results]

    def get_all_gpu_resources(self) -> List[Dict[str, Any]]:
        """Get all GPU resources."""
        results = self._execute_with_metrics(
            "SELECT * FROM gpu_resources ORDER BY pueue_group", ()
        )
        return [dict(row) for row in results]

    def update_gpu_status(
        self, pueue_group: str, status: str, case_id: Optional[int] = None
    ) -> None:
        """Update GPU resource status."""
        with self.transaction():
            self.cursor.execute(
                """
                UPDATE gpu_resources
                SET status = ?, assigned_case_id = ?, last_updated = CURRENT_TIMESTAMP
                WHERE pueue_group = ?
                """,
                (status, case_id, pueue_group),
            )

        if self.enable_cache:
            self.query_cache.invalidate("gpu_resources")

    def close(self) -> None:
        """Close database connection."""
        if hasattr(self, "conn") and self.conn:
            self.conn.close()
