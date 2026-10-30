
import heapq
import math
import threading
import time

from simulator.models import Job, JobStatus


class QueueManager:
    """Thread-safe priority queue for printing jobs.

    Lower priority numbers represent more urgent jobs.
    Jobs with equal effective priority are processed in FIFO order.
    """

    def __init__(self, aging_interval: float | None = None) -> None:
        if aging_interval is not None and (
            isinstance(aging_interval, bool)
            or not isinstance(aging_interval, (int, float))
            or not math.isfinite(aging_interval)
            or aging_interval <= 0
        ):
            raise ValueError(
                "Aging interval must be positive and finite."
            )

        self._heap: list[tuple[int, int, str]] = []
        self._jobs: dict[str, Job] = {}
        self._sequence = 0
        self._lock = threading.Lock()
        self._insertion_order: dict[str, int] = {}
        self._enqueued_at: dict[str, float] = {}
        self.aging_interval = (
            float(aging_interval)
            if aging_interval is not None else None
        )

    def enqueue(self, job: Job) -> None:
        """Add a job to the queue."""
        with self._lock:
            if job.id in self._jobs:
                raise ValueError(f"Job ID already exists: {job.id}")

            if job.status != JobStatus.QUEUED:
                raise ValueError("Only queued jobs can be enqueued.")

            heapq.heappush(
                self._heap,
                (job.priority, self._sequence, job.id),
            )
            self._jobs[job.id] = job
            self._insertion_order[job.id] = self._sequence
            self._enqueued_at[job.id] = time.monotonic()
            self._sequence += 1

    def cancel(self, job_id: str) -> bool:
        """Cancel a queued job. Return False if it cannot be cancelled."""
        with self._lock:
            job = self._jobs.get(job_id)

            if job is None or job.status != JobStatus.QUEUED:
                return False

            job.status = JobStatus.CANCELLED
            job.finished_at = time.time()
            return True

    def get_next_job(self) -> Job | None:
        """Return the highest-priority queued job, or None if empty."""
        return self.claim_next_job()

    def _effective_priority(
        self,
        job: Job,
        current_time: float,
    ) -> int:
        """Calculate a job's priority based on its queue waiting time."""
        if self.aging_interval is None:
            return job.priority

        waiting_time = current_time - self._enqueued_at[job.id]
        priority_improvement = math.floor(
            waiting_time / self.aging_interval
        )

        return max(0, job.priority - priority_improvement)

    def claim_next_job(self) -> Job | None:
        """Atomically claim the next queued job for a printer."""
        with self._lock:
            if self.aging_interval is not None:
                current_time = time.monotonic()

                self._heap = [
                    (
                        self._effective_priority(job, current_time),
                        self._insertion_order[job.id],
                        job.id,
                    )
                    for job in self._jobs.values()
                    if job.status == JobStatus.QUEUED
                ]
                heapq.heapify(self._heap)

            while self._heap:
                effective_priority, _, job_id = heapq.heappop(
                    self._heap
                )
                job = self._jobs[job_id]

                if job.status != JobStatus.QUEUED:
                    continue

                if self.aging_interval is None:
                    effective_priority = job.priority

                job.effective_priority_at_start = effective_priority
                job.status = JobStatus.RUNNING
                job.started_at = time.time()
                return job

            return None

    def list_jobs(self) -> list[Job]:
        """Return all jobs ordered by their insertion sequence."""
        with self._lock:
            return sorted(
                self._jobs.values(),
                key=lambda job: self._insertion_order[job.id],
            )

    def queued_count(self) -> int:
        """Return the number of jobs waiting to be assigned."""
        with self._lock:
            return sum(
                job.status == JobStatus.QUEUED
                for job in self._jobs.values()
            )
