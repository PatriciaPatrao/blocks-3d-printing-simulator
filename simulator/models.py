
from dataclasses import dataclass, field
from enum import StrEnum
import time


class JobStatus(StrEnum):
    QUEUED = "queued"
    RUNNING = "running"
    COMPLETED = "completed"
    CANCELLED = "cancelled"
    FAILED = "failed"


@dataclass
class Job:
    id: str
    material: str
    est_time: float
    priority: int = 0
    created_at: float = field(default_factory=time.time)

    started_at: float | None = None
    finished_at: float | None = None
    status: JobStatus = JobStatus.QUEUED

    def __post_init__(self) -> None:
        if not self.id.strip():
            raise ValueError("Job id cannot be empty.")

        if not self.material.strip():
            raise ValueError("Job material cannot be empty.")

        if isinstance(self.est_time, bool) or not isinstance(
            self.est_time, (int, float)
        ):
            raise ValueError("Estimated time must be a number.")

        if self.est_time <= 0 or self.est_time == float("inf"):
            raise ValueError("Estimated time must be positive and finite.")

        if self.est_time != self.est_time:
            raise ValueError("Estimated time cannot be NaN.")

        if isinstance(self.priority, bool) or not isinstance(
            self.priority, int
        ):
            raise ValueError("Priority must be an integer.")

        if isinstance(self.created_at, bool) or not isinstance(
            self.created_at, (int, float)
        ):
            raise ValueError("Created_at must be a timestamp.")

        if not (float("-inf") < self.created_at < float("inf")):
            raise ValueError("Created_at must be finite.")


@dataclass
class Printer:
    id: str
    current_job_id: str | None = None
    busy_time: float = 0.0

    def __post_init__(self) -> None:
        if not self.id.strip():
            raise ValueError("Printer id cannot be empty.")

        if self.busy_time < 0 or self.busy_time == float("inf"):
            raise ValueError("Busy time must be non-negative and finite.")

        if self.busy_time != self.busy_time:
            raise ValueError("Busy time cannot be NaN.")

    @property
    def is_available(self) -> bool:
        return self.current_job_id is None
