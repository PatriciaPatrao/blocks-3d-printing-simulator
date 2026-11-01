
import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator


class Database:
    """Manage SQLite database connections, schema and transactions."""

    def __init__(self, database_path: Path | str | None = None) -> None:
        project_root = Path(__file__).resolve().parent.parent

        self.database_path = (
            Path(database_path)
            if database_path is not None
            else project_root / "data" / "simulator.db"
        )

        self.database_path.parent.mkdir(
            parents=True,
            exist_ok=True,
        )

        self.initialize()

    def connect(self) -> sqlite3.Connection:
        """Create a configured database connection."""
        connection = sqlite3.connect(
            self.database_path,
            timeout=10,
        )
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute("PRAGMA busy_timeout = 10000")

        return connection

    def initialize(self) -> None:
        """Create the database schema if it does not exist."""
        connection = self.connect()

        try:
            # WAL mode must be configured outside a transaction.
            connection.execute("PRAGMA journal_mode = WAL")

            # Keep schema creation and migrations atomic.
            connection.executescript(
                """
                BEGIN IMMEDIATE;

                CREATE TABLE IF NOT EXISTS simulation_runs (
                    id TEXT PRIMARY KEY,
                    started_at REAL NOT NULL,
                    finished_at REAL,
                    status TEXT NOT NULL CHECK (
                        status IN ('running', 'completed', 'failed')
                    ),
                    input_file TEXT,
                    printer_count INTEGER NOT NULL CHECK (
                        printer_count > 0
                    ),
                    time_scale REAL NOT NULL CHECK (
                        time_scale > 0
                    ),
                    aging_interval REAL CHECK (
                        aging_interval IS NULL OR aging_interval > 0
                    ),
                    elapsed_time REAL,
                    metrics_json TEXT,
                    error_message TEXT
                );

                CREATE TABLE IF NOT EXISTS jobs (
                    run_id TEXT NOT NULL,
                    job_id TEXT NOT NULL,
                    material TEXT NOT NULL,
                    est_time REAL NOT NULL CHECK (est_time > 0),
                    priority INTEGER NOT NULL,
                    effective_priority_at_start INTEGER,
                    status TEXT NOT NULL CHECK (
                        status IN (
                            'queued',
                            'running',
                            'completed',
                            'cancelled',
                            'failed'
                        )
                    ),
                    created_at REAL NOT NULL,
                    started_at REAL,
                    finished_at REAL,

                    PRIMARY KEY (run_id, job_id),

                    FOREIGN KEY (run_id)
                        REFERENCES simulation_runs(id)
                        ON DELETE RESTRICT
                );

                CREATE TABLE IF NOT EXISTS job_status_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_id TEXT NOT NULL,
                    job_id TEXT NOT NULL,
                    status TEXT NOT NULL CHECK (
                        status IN (
                            'queued',
                            'running',
                            'completed',
                            'cancelled',
                            'failed'
                        )
                    ),
                    changed_at REAL NOT NULL,

                    FOREIGN KEY (run_id, job_id)
                        REFERENCES jobs(run_id, job_id)
                        ON DELETE RESTRICT
                );

                CREATE INDEX IF NOT EXISTS idx_jobs_run_status
                    ON jobs(run_id, status);

                CREATE INDEX IF NOT EXISTS idx_runs_started_at
                    ON simulation_runs(started_at);

                CREATE INDEX IF NOT EXISTS idx_job_history_lookup
                    ON job_status_history(run_id, job_id, id DESC);
                """
            )

            self._migrate_schema(connection)
            connection.commit()

        except BaseException:
            connection.rollback()
            raise

        finally:
            connection.close()

    def _migrate_schema(
        self,
        connection: sqlite3.Connection,
    ) -> None:
        """Apply schema migrations to existing databases."""
        columns = {
            row["name"]
            for row in connection.execute(
                "PRAGMA table_info(jobs)"
            ).fetchall()
        }

        migrations = {
            "wait_time_seconds": "REAL",
            "run_time_seconds": "REAL",
        }

        for column_name, column_type in migrations.items():
            if column_name not in columns:
                connection.execute(
                    f"ALTER TABLE jobs ADD COLUMN "
                    f"{column_name} {column_type}"
                )

    @contextmanager
    def transaction(self) -> Iterator[sqlite3.Connection]:
        """Provide an atomic database transaction."""
        connection = self.connect()

        try:
            yield connection
            connection.commit()

        except BaseException:
            try:
                connection.rollback()
            except sqlite3.Error:
                pass
            raise

        finally:
            connection.close()
