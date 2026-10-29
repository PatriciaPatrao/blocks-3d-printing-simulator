
import asyncio
import time
from datetime import datetime, timezone
from statistics import mean, median

from simulator.models import Job, JobStatus, Printer
from simulator.queue_manager import QueueManager


def iso_timestamp(timestamp: float | None) -> str | None:
    """Convert a Unix timestamp to an ISO 8601 UTC string."""
    if timestamp is None:
        return None

    return datetime.fromtimestamp(
        timestamp, tz=timezone.utc
    ).isoformat()


class Simulator:
    def __init__(
        self,
        printer_count: int = 2,
        time_scale: float = 0.01,
    ) -> None:
        if (
            isinstance(printer_count, bool)
            or not isinstance(printer_count, int)
            or printer_count < 1
        ):
            raise ValueError("printer_count must be a positive integer.")

        if (
            isinstance(time_scale, bool)
            or not isinstance(time_scale, (int, float))
            or not 0 < time_scale < float("inf")
        ):
            raise ValueError("time_scale must be positive and finite.")

        self.queue = QueueManager()
        self.printers = [
            Printer(id=f"P{i + 1}") for i in range(printer_count)
        ]
        self.time_scale = float(time_scale)
        self._jobs: dict[str, Job] = {}
        self._simulation_elapsed = 0.0
        self.last_report: dict | None = None
        self._stop_claiming = False
        self._has_run = False

    def add_job(self, job: Job) -> None:
        self.queue.enqueue(job)
        self._jobs[job.id] = job

    def cancel_job(self, job_id: str) -> bool:
        return self.queue.cancel(job_id)

    def list_jobs(self) -> list[Job]:
        return list(self._jobs.values())


    async def _printer_worker(
        self,
        printer: Printer,
        initial_job: Job | None = None,
    ) -> None:
        job = initial_job

        while True:
            if self._stop_claiming:
                return

            if job is None:
                job = self.queue.claim_next_job()

            if job is None:
                return

            printer.current_job_id = job.id
            job_started = time.monotonic()

            try:
                await asyncio.sleep(job.est_time * self.time_scale)
            except Exception:
                self._stop_claiming = True
                job.status = JobStatus.FAILED
                job.finished_at = time.time()
                raise
            else:
                job.status = JobStatus.COMPLETED
                job.finished_at = time.time()
            finally:
                printer.busy_time += time.monotonic() - job_started
                printer.current_job_id = None

            job = None

    async def run(self) -> dict:
        if self._has_run:
            raise RuntimeError("A simulation instance can only run once.")

        self._has_run = True
        self._stop_claiming = False

        started = time.monotonic()

        # Assign one initial job to each printer before workers start.
        initial_jobs = [
            self.queue.claim_next_job()
            for _ in self.printers
        ]

        results = await asyncio.gather(
            *(
                self._printer_worker(printer, initial_job)
                for printer, initial_job in zip(
                    self.printers,
                    initial_jobs,
                )
            ),
            return_exceptions=True,
        )

        self._simulation_elapsed = time.monotonic() - started
        self.last_report = self.generate_report()

        errors = [
            result for result in results
            if isinstance(result, Exception)
        ]

        if errors:
            raise errors[0]

        return self.last_report



    def generate_report(self) -> dict:
        jobs = list(self._jobs.values())
        completed = [
            job for job in jobs
            if job.status == JobStatus.COMPLETED
        ]

        wait_times = [
            job.started_at - job.created_at
            for job in jobs
            if job.started_at is not None
        ]

        elapsed = self._simulation_elapsed
        total_occupancy = sum(
            printer.busy_time for printer in self.printers
        )

        return {
            "generated_at": iso_timestamp(time.time()),
            "simulation_elapsed_seconds": round(elapsed, 6),
            "jobs": [
                {
                    "id": job.id,
                    "material": job.material,
                    "est_time": job.est_time,
                    "priority": job.priority,
                    "status": job.status.value,
                    "created_at": iso_timestamp(job.created_at),
                    "started_at": iso_timestamp(job.started_at),
                    "finished_at": iso_timestamp(job.finished_at),
                    "wait_time": (
                        round(job.started_at - job.created_at, 6)
                        if job.started_at is not None else None
                    ),
                    "run_time": (
                        round(job.finished_at - job.started_at, 6)
                        if job.finished_at is not None
                        and job.started_at is not None else None
                    ),
                }
                for job in jobs
            ],
            "metrics": {
                "total_jobs": len(jobs),
                "completed_jobs": len(completed),
                "queued_jobs": sum(
                    job.status == JobStatus.QUEUED for job in jobs
                ),
                "cancelled_jobs": sum(
                    job.status == JobStatus.CANCELLED for job in jobs
                ),
                "average_wait_time_seconds": round(
                    mean(wait_times), 6
                ) if wait_times else 0,
                "median_wait_time_seconds": round(
                    median(wait_times), 6
                ) if wait_times else 0,
                "throughput_jobs_per_second": (
                    round(len(jobs) / elapsed, 6)
                    if elapsed > 0
                    else 0.0
                ),
                "completed_jobs_per_second": (
                    round(len(completed) / elapsed, 6)
                    if elapsed > 0
                    else 0.0
                ),
                "total_printer_occupancy_seconds": round(
                    total_occupancy, 6
                ),
                "printer_occupancy_seconds": {
                    printer.id: round(printer.busy_time, 6)
                    for printer in self.printers
                },
                "failed_jobs": sum(
                    job.status == JobStatus.FAILED for job in jobs
                ),
            },
        }
