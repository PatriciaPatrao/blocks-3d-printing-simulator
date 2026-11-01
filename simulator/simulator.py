
import asyncio
import time
from dataclasses import replace
from datetime import datetime, timezone
from statistics import mean, median

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
        """Add a job before the simulation starts."""
        if self._has_run:
            raise RuntimeError(
                "Cannot add jobs after the simulation has started."
            )

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

    async def _printer_worker(self, printer: Printer) -> None:
        """Process queued jobs assigned to a printer."""
        while True:
            if self._stop_claiming:
                return

            job = self._claim_next_job()

            if job is None:
                return

            # Register cancellation for every claimed job.
            cancel_event = asyncio.Event()
            self._running_cancellations[job.id] = cancel_event

            printer.current_job_id = job.id
            job_started = self._job_started_at[job.id]
            worker_error: BaseException | None = None

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

            except asyncio.CancelledError as error:
                worker_error = error
                self._stop_claiming = True
                job.status = JobStatus.FAILED
                job.finished_at = time.time()
                raise

            except Exception as error:
                worker_error = error
                self._stop_claiming = True
                job.status = JobStatus.FAILED
                job.finished_at = time.time()
                raise

            finally:
                for task in (sleep_task, cancel_task):
                    if not task.done():
                        task.cancel()

                try:
                    await asyncio.gather(
                        sleep_task,
                        cancel_task,
                        return_exceptions=True,
                    )
                finally:
                    run_time = time.monotonic() - job_started

                    job.run_time_seconds = run_time
                    self._job_started_at.pop(job.id, None)

                    self._running_cancellations.pop(job.id, None)
                    printer.busy_time += run_time
                    printer.current_job_id = None

                    # Record the final status without masking the original error.
                    try:
                        self._record_status(job)
                    except Exception as persistence_error:
                        self._stop_claiming = True

                        if worker_error is None:
                            raise

                        worker_error.add_note(
                            "Failed to persist the final job status: "
                            f"{persistence_error}"
                        )

    def _fail_interrupted_jobs(self) -> list[Exception]:
        """Mark jobs left running after an external worker cancellation."""
        persistence_errors: list[Exception] = []

        for job in self._jobs.values():
            if job.status != JobStatus.RUNNING:
                continue

            job.status = JobStatus.FAILED
            job.finished_at = time.time()

            job_started = self._job_started_at.pop(job.id, None)

            if job_started is not None:
                run_time = time.monotonic() - job_started

                active_printer = next(
                    (
                        printer
                        for printer in self.printers
                        if printer.current_job_id == job.id
                    ),
                    None,
                )

                if active_printer is not None:
                    job.run_time_seconds = run_time
                    active_printer.busy_time += run_time
                    active_printer.current_job_id = None
                else:
                    job.run_time_seconds = 0.0

            self._running_cancellations.pop(job.id, None)

            try:
                self._record_status(job)
            except Exception as error:
                persistence_errors.append(error)

        return persistence_errors

    async def run(self) -> dict:
        """Run the simulation."""
        if self._has_run:
            raise RuntimeError("A simulation instance can only run once.")

        self._has_run = True
        self._stop_claiming = False

        started = time.monotonic()

        # Start persisting the simulation state.
        try:
            self._start_persistence()

        except Exception as error:
            self._simulation_elapsed = time.monotonic() - started
            self.last_report = self.generate_report()

            if self.repository is not None and self.run_id is not None:
                try:
                    self.repository.finish_run(
                        run_id=self.run_id,
                        status="failed",
                        elapsed_time=self._simulation_elapsed,
                        metrics=self.last_report["metrics"],
                        error_message=f"{type(error).__name__}: {error}",
                    )
                except Exception as persistence_error:
                    error.add_note(
                        "Failed to persist the simulation failure status: "
                        f"{persistence_error}"
                    )

            raise

        # Create one worker per printer.
        worker_tasks = [
            asyncio.create_task(self._printer_worker(printer))
            for printer in self.printers
        ]

        run_cancelled: asyncio.CancelledError | None = None

        try:
            results = await asyncio.gather(
                *worker_tasks,
                return_exceptions=True,
            )

        except asyncio.CancelledError as error:
            # Stop claiming jobs and wait for worker cleanup.
            run_cancelled = error
            self._stop_claiming = True

            cleanup_task = asyncio.gather(
                *worker_tasks,
                return_exceptions=True,
            )

            # Keep waiting even if the simulation is cancelled again.
            while not cleanup_task.done():
                try:
                    await asyncio.shield(cleanup_task)
                except asyncio.CancelledError:
                    continue

            results = cleanup_task.result()

        errors = [
            result
            for result in results
            if isinstance(result, BaseException)
        ]

        # Handle workers cancelled before they could clean up their jobs.
        if (
            run_cancelled is not None
            or any(
                isinstance(error, asyncio.CancelledError)
                for error in errors
            )
        ):
            self._stop_claiming = True
            errors.extend(self._fail_interrupted_jobs())

        if run_cancelled is not None:
            errors.insert(0, run_cancelled)

        # Generate the final report and persist the simulation state.
        self._simulation_elapsed = time.monotonic() - started
        self.last_report = self.generate_report()

        if self.repository is not None:
            status = "failed" if errors else "completed"

            if run_cancelled is not None:
                error_message = "Simulation task was cancelled."
            elif errors:
                error_message = (
                    f"{type(errors[0]).__name__}: {errors[0]}"
                )
            else:
                error_message = None

            try:
                self.repository.finish_run(
                    run_id=self.run_id,
                    status=status,
                    elapsed_time=self._simulation_elapsed,
                    metrics=self.last_report["metrics"],
                    error_message=error_message,
                )
            except Exception as persistence_error:
                if not errors:
                    raise

                errors[0].add_note(
                    "Failed to persist the final simulation state: "
                    f"{persistence_error}"
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
        """Claim the next queued job from the queue."""
        job = self.queue.claim_next_job()

        if job is None:
            return None

        now = time.monotonic()
        queued_at = self._job_queued_at.pop(job.id)

        job.wait_time_seconds = now - queued_at
        self._job_started_at[job.id] = now

        try:
            self._record_status(job)

        except Exception as error:
            self._stop_claiming = True

            job.status = JobStatus.FAILED
            job.finished_at = time.time()
            job.run_time_seconds = 0.0

            self._job_started_at.pop(job.id, None)

            try:
                self._record_status(job)
            except Exception as persistence_error:
                error.add_note(
                    "Failed to persist the job failure status: "
                    f"{persistence_error}"
                )

            raise

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
