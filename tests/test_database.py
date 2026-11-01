
import sqlite3

from simulator.database import Database


def test_database_migrates_legacy_schema_without_losing_data(tmp_path):
    database_path = tmp_path / "legacy.db"

    # Create a legacy jobs table without duration columns.
    with sqlite3.connect(database_path) as connection:
        connection.execute(
            """
            CREATE TABLE jobs (
                run_id TEXT NOT NULL,
                job_id TEXT NOT NULL,
                material TEXT NOT NULL,
                est_time REAL NOT NULL,
                priority INTEGER NOT NULL,
                effective_priority_at_start INTEGER,
                status TEXT NOT NULL,
                created_at REAL NOT NULL,
                started_at REAL,
                finished_at REAL,
                PRIMARY KEY (run_id, job_id)
            )
            """
        )

        connection.execute(
            """
            INSERT INTO jobs (
                run_id,
                job_id,
                material,
                est_time,
                priority,
                status,
                created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            ("legacy-run", "JOB-001", "PLA", 30.0, 1, "queued", 1000.0),
        )

    # Initializing the database should apply the migration.
    database = Database(database_path)

    with database.connect() as connection:
        columns = {
            row["name"]
            for row in connection.execute(
                "PRAGMA table_info(jobs)"
            ).fetchall()
        }

        job = connection.execute(
            "SELECT * FROM jobs WHERE run_id = ? AND job_id = ?",
            ("legacy-run", "JOB-001"),
        ).fetchone()

    assert "wait_time_seconds" in columns
    assert "run_time_seconds" in columns

    # Verify that the existing record was preserved.
    assert job["material"] == "PLA"
    assert job["est_time"] == 30.0
    assert job["priority"] == 1
    assert job["status"] == "queued"
    assert job["created_at"] == 1000.0

    # New duration columns should default to NULL.
    assert job["wait_time_seconds"] is None
    assert job["run_time_seconds"] is None
