import json
import time
import uuid

from contextlib import closing
from simulator.database import Database
from simulator.models import Job, JobStatus


class JobRepository:
    """Persist simulation runs and their associated jobs."""

    def __init__(self, database: Database) -> None:
        self.database = database


    @staticmethod
    def _get_status_timestamp(job: Job) -> float:
        """Get the timestamp associated with the current job status."""
        if job.status == JobStatus.QUEUED:
            return job.created_at

        if job.status == JobStatus.RUNNING:
            return (
                job.started_at
                if job.started_at is not None
                else time.time()
            )

        return (
            job.finished_at
            if job.finished_at is not None
            else time.time()
        )

    def create_run(
        self,
        printer_count: int,
        time_scale: float,
        aging_interval: float | None,
        input_file: str | None = None,
    ) -> str:
        """Create a new simulation run and return its ID."""
        run_id = str(uuid.uuid4())

        with self.database.transaction() as connection:
            connection.execute(
                """
                INSERT INTO simulation_runs (
                    id,
                    started_at,
                    status,
                    input_file,
                    printer_count,
                    time_scale,
                    aging_interval
                )
                VALUES (?, ?, 'running', ?, ?, ?, ?)
                """,
                (
                    run_id,
                    time.time(),
                    input_file,
                    printer_count,
                    time_scale,
                    aging_interval,
                ),
            )

        return run_id


    def add_job(self, run_id: str, job: Job) -> None:
        """Persist a job and its initial status."""
        with self.database.transaction() as connection:
            connection.execute(
                """
                INSERT INTO jobs (
                    run_id,
                    job_id,
                    material,
                    est_time,
                    priority,
                    effective_priority_at_start,
                    status,
                    created_at,
                    started_at,
                    finished_at,
                    wait_time_seconds,
                    run_time_seconds
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    run_id,
                    job.id,
                    job.material,
                    job.est_time,
                    job.priority,
                    job.effective_priority_at_start,
                    job.status.value,
                    job.created_at,
                    job.started_at,
                    job.finished_at,
                    job.wait_time_seconds,
                    job.run_time_seconds,
                ),
            )

            connection.execute(
                """
                INSERT INTO job_status_history (
                    run_id,
                    job_id,
                    status,
                    changed_at
                )
                VALUES (?, ?, ?, ?)
                """,
                (
                    run_id,
                    job.id,
                    job.status.value,
                    self._get_status_timestamp(job)
                ),
            )

    def update_job(self, run_id: str, job: Job) -> None:
        """Update a job and record status changes."""
        with self.database.transaction() as connection:
            cursor = connection.execute(
                """
                SELECT status
                FROM jobs
                WHERE run_id = ? AND job_id = ?
                """,
                (run_id, job.id),
            )

            existing = cursor.fetchone()

            if existing is None:
                raise ValueError(
                    f"Job not found in simulation run: {job.id}"
                )

            connection.execute(
                """
                UPDATE jobs
                SET material = ?,
                    est_time = ?,
                    priority = ?,
                    effective_priority_at_start = ?,
                    status = ?,
                    created_at = ?,
                    started_at = ?,
                    finished_at = ?,
                    wait_time_seconds = ?,
                    run_time_seconds = ?
                WHERE run_id = ? AND job_id = ?
                """,
                (
                    job.material,
                    job.est_time,
                    job.priority,
                    job.effective_priority_at_start,
                    job.status.value,
                    job.created_at,
                    job.started_at,
                    job.finished_at,
                    job.wait_time_seconds,
                    job.run_time_seconds,
                    run_id,
                    job.id,
                ),
            )

            if existing["status"] != job.status.value:
                connection.execute(
                    """
                    INSERT INTO job_status_history (
                        run_id,
                        job_id,
                        status,
                        changed_at
                    )
                    VALUES (?, ?, ?, ?)
                    """,
                    (
                        run_id,
                        job.id,
                        job.status.value,
                        self._get_status_timestamp(job)
                    ),
                )

    def finish_run(
        self,
        run_id: str,
        status: str,
        elapsed_time: float,
        metrics: dict,
        error_message: str | None = None,
    ) -> None:
        """Mark a simulation run as finished."""
        if status not in {"completed", "failed"}:
            raise ValueError("Invalid simulation run status.")

        with self.database.transaction() as connection:
            cursor = connection.execute(
                """
                UPDATE simulation_runs
                SET finished_at = ?,
                    status = ?,
                    elapsed_time = ?,
                    metrics_json = ?,
                    error_message = ?
                WHERE id = ? AND status = 'running'
                """,
                (
                    time.time(),
                    status,
                    elapsed_time,
                    json.dumps(metrics),
                    error_message,
                    run_id,
                ),
            )

            if cursor.rowcount != 1:
                raise ValueError(
                    f"Active simulation run not found: {run_id}"
                )

    def get_run(self, run_id: str) -> dict | None:
        """Retrieve a simulation run and its jobs."""
        with closing(self.database.connect()) as connection:
            run = connection.execute(
                """
                SELECT *
                FROM simulation_runs
                WHERE id = ?
                """,
                (run_id,),
            ).fetchone()

            if run is None:
                return None

            jobs = connection.execute(
                """
                SELECT *
                FROM jobs
                WHERE run_id = ?
                ORDER BY rowid
                """,
                (run_id,),
            ).fetchall()

            return {
                "run": dict(run),
                "jobs": [dict(job) for job in jobs],
            }

    def list_runs(self) -> list[dict]:
        """List simulation runs, newest first."""
        with closing(self.database.connect()) as connection:
            rows = connection.execute(
                """
                SELECT *
                FROM simulation_runs
                ORDER BY started_at DESC
                """
            ).fetchall()

            return [dict(row) for row in rows]

    def get_job_history(
        self,
        run_id: str,
        job_id: str,
    ) -> list[dict]:
        """Retrieve all status changes for a job."""
        with closing(self.database.connect()) as connection:
            rows = connection.execute(
                """
                SELECT status, changed_at
                FROM job_status_history
                WHERE run_id = ? AND job_id = ?
                ORDER BY id
                """,
                (run_id, job_id),
            ).fetchall()

            return [dict(row) for row in rows]

