
import heapq
import threading
import time

from simulator.models import Job, JobStatus


class QueueManager:
    """Thread-safe priority queue for printing jobs.

    Lower priority numbers represent more urgent jobs.
    Jobs with equal priority are processed in FIFO order.
    """

    def __init__(self) -> None:
        self._heap: list[tuple[int, int, str]] = []
        self._jobs: dict[str, Job] = {}
        self._sequence = 0
        self._lock = threading.Lock()
        self._insertion_order: dict[str, int] = {}

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
            self._sequence += 1
            self._insertion_order[job.id] = self._sequence

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
        with self._lock:
            while self._heap:
                _, _, job_id = heapq.heappop(self._heap)
                job = self._jobs[job_id]

                # Cancelled jobs remain in the heap until they reach the top.
                if job.status == JobStatus.QUEUED:
                    return job

            return None

    def claim_next_job(self) -> Job | None:
        """Atomically claim the next queued job for a printer."""
        with self._lock:
            while self._heap:
                _, _, job_id = heapq.heappop(self._heap)
                job = self._jobs[job_id]

                if job.status != JobStatus.QUEUED:
                    continue

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
