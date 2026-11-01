import asyncio
import time
from datetime import datetime, timezone
from statistics import mean, median
from dataclasses import replace

from simulator.models import Job, JobStatus, Printer
from simulator.queue_manager import QueueManager
from simulator.repositories.job_repository import JobRepository


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
        aging_interval: float | None = None,
        repository: JobRepository | None = None,
        input_file: str | None = None,
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

        self.queue = QueueManager(aging_interval=aging_interval)
        self.aging_interval = self.queue.aging_interval
        self.printers = [
            Printer(id=f"P{i + 1}") for i in range(printer_count)
        ]
        self.time_scale = float(time_scale)
        self._jobs: dict[str, Job] = {}
        self._simulation_elapsed = 0.0
        self.last_report: dict | None = None
        self._stop_claiming = False
        self._has_run = False
        self._running_cancellations: dict[str, asyncio.Event] = {}
        self.repository = repository
        self.input_file = input_file
        self.run_id: str | None = None
        self._status_events: list[tuple[str, Job]] = []
        self._job_queued_at: dict[str, float] = {}
        self._job_started_at: dict[str, float] = {}

    def add_job(self, job: Job) -> None:
        self.queue.enqueue(job)
        self._jobs[job.id] = job
        self._job_queued_at[job.id] = time.monotonic()
        self._record_status(job)

    def cancel_job(self, job_id: str) -> bool:
        """Request cancellation of a queued or running job."""
        if self.queue.cancel(job_id):
            self._record_status(self._jobs[job_id])
            return True

        job = self._jobs.get(job_id)

        if job is None or job.status != JobStatus.RUNNING:
            return False

        cancel_event = self._running_cancellations.get(job_id)

        if cancel_event is None or cancel_event.is_set():
            return False

        cancel_event.set()
        return True

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
                job = self._claim_next_job()

            if job is None:
                return

            # Register cancellation for every claimed job.
            cancel_event = asyncio.Event()
            self._running_cancellations[job.id] = cancel_event

            printer.current_job_id = job.id
            job_started = self._job_started_at[job.id]

            sleep_task = asyncio.create_task(
                asyncio.sleep(job.est_time * self.time_scale)
            )
            cancel_task = asyncio.create_task(cancel_event.wait())

            try:
                done, _ = await asyncio.wait(
                    {sleep_task, cancel_task},
                    return_when=asyncio.FIRST_COMPLETED,
                )

                # If both finish simultaneously, completion takes precedence.
                if sleep_task in done:
                    await sleep_task
                    job.status = JobStatus.COMPLETED
                else:
                    job.status = JobStatus.CANCELLED

                job.finished_at = time.time()

            except Exception:
                self._stop_claiming = True
                job.status = JobStatus.FAILED
                job.finished_at = time.time()
                raise

            finally:
                for task in (sleep_task, cancel_task):
                    if not task.done():
                        task.cancel()

                await asyncio.gather(
                    sleep_task,
                    cancel_task,
                    return_exceptions=True,
                )

                job.run_time_seconds = time.monotonic() - job_started
                self._job_started_at.pop(job.id, None)

                self._running_cancellations.pop(job.id, None)
                printer.busy_time += time.monotonic() - job_started
                printer.current_job_id = None
                # Record the final status of the job.
                self._record_status(job)

            # Continue with the next queued job, even after cancellation.
            job = None

    async def run(self) -> dict:
        """Run the simulation."""
        if self._has_run:
            raise RuntimeError("A simulation instance can only run once.")

        self._has_run = True
        self._stop_claiming = False
        # Start persisting the simulation state.
        self._start_persistence()

        started = time.monotonic()

        # Assign one initial job to each printer before workers start.
        initial_jobs = [
            self._claim_next_job()
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

        # Generate the final report and persist the simulation state.
        self._simulation_elapsed = time.monotonic() - started
        self.last_report = self.generate_report()

        errors = [
            result for result in results
            if isinstance(result, Exception)
        ]

        if self.repository is not None:
            status = "failed" if errors else "completed"
            error_message = (
                f"{type(errors[0]).__name__}: {errors[0]}"
                if errors else None
            )

            self.repository.finish_run(
                run_id=self.run_id,
                status=status,
                elapsed_time=self._simulation_elapsed,
                metrics=self.last_report["metrics"],
                error_message=error_message,
            )

        if errors:
            raise errors[0]

        return self.last_report

    def _record_status(self, job: Job) -> None:
        """Record a snapshot of a job's current state."""
        if self.repository is None:
            return

        snapshot = replace(job)

        if self.run_id is None:
            self._status_events.append((job.id, snapshot))
            return

        self.repository.update_job(self.run_id, snapshot)

    def _start_persistence(self) -> None:
        """Create a run and persist all jobs and pre-run status changes."""
        if self.repository is None:
            return

        self.run_id = self.repository.create_run(
            printer_count=len(self.printers),
            time_scale=self.time_scale,
            aging_interval=self.aging_interval,
            input_file=self.input_file,
        )

        persisted_jobs: set[str] = set()

        for job_id, snapshot in self._status_events:
            if job_id not in persisted_jobs:
                self.repository.add_job(self.run_id, snapshot)
                persisted_jobs.add(job_id)
            else:
                self.repository.update_job(self.run_id, snapshot)

        self._status_events.clear()

    def _claim_next_job(self) -> Job | None:
        """Claim the next job from the queue."""
        job = self.queue.claim_next_job()

        if job is not None:
            now = time.monotonic()
            queued_at = self._job_queued_at.pop(job.id)

            job.wait_time_seconds = now - queued_at
            self._job_started_at[job.id] = now

            self._record_status(job)

        return job

    def generate_report(self) -> dict:
        jobs = list(self._jobs.values())
        completed = [
            job for job in jobs
            if job.status == JobStatus.COMPLETED
        ]

        wait_times = [
            job.wait_time_seconds
            for job in jobs
            if job.wait_time_seconds is not None
        ]

        elapsed = self._simulation_elapsed
        total_occupancy = sum(
            printer.busy_time for printer in self.printers
        )

        return {
            "generated_at": iso_timestamp(time.time()),
            "simulation_elapsed_seconds": round(elapsed, 6),
            "configuration": {
                "aging_enabled": self.aging_interval is not None,
                "aging_interval_seconds": self.aging_interval,
            },
            "jobs": [
                {
                    "id": job.id,
                    "material": job.material,
                    "est_time": job.est_time,
                    "priority": job.priority,
                    "effective_priority_at_start": (
                        job.effective_priority_at_start
                    ),
                    "status": job.status.value,
                    "created_at": iso_timestamp(job.created_at),
                    "started_at": iso_timestamp(job.started_at),
                    "finished_at": iso_timestamp(job.finished_at),
                    "wait_time": (
                        round(job.wait_time_seconds, 6)
                        if job.wait_time_seconds is not None else None
                    ),
                    "run_time": (
                        round(job.run_time_seconds, 6)
                        if job.run_time_seconds is not None else None
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
