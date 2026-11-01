import math
import time

from dataclasses import dataclass, field
from enum import StrEnum


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
    wait_time_seconds: float | None = None
    run_time_seconds: float | None = None
    status: JobStatus = JobStatus.QUEUED
    effective_priority_at_start: int | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.id, str) or not self.id.strip():
            raise ValueError("Job id must be a non-empty string.")

        if not isinstance(self.material, str) or not self.material.strip():
            raise ValueError("Job material must be a non-empty string.")

        if (
            isinstance(self.est_time, bool)
            or not isinstance(self.est_time, (int, float))
            or not math.isfinite(self.est_time)
            or self.est_time <= 0
        ):
            raise ValueError("Estimated time must be positive and finite.")

        if isinstance(self.priority, bool) or not isinstance(self.priority, int):
            raise ValueError("Priority must be an integer.")

        if (
            isinstance(self.created_at, bool)
            or not isinstance(self.created_at, (int, float))
            or not math.isfinite(self.created_at)
        ):
            raise ValueError("Created_at must be a finite timestamp.")

        if not isinstance(self.status, JobStatus):
            raise ValueError("Invalid job status.")

        for name in ("started_at", "finished_at"):
            value = getattr(self, name)

            if value is not None and (
                isinstance(value, bool)
                or not isinstance(value, (int, float))
                or not math.isfinite(value)
            ):
                raise ValueError(
                    f"{name} must be a finite timestamp or None."
                )

        for name in ("wait_time_seconds", "run_time_seconds"):
            value = getattr(self, name)

            if value is not None and (
                isinstance(value, bool)
                or not isinstance(value, (int, float))
                or not math.isfinite(value)
                or value < 0
            ):
                raise ValueError(
                    f"{name} must be non-negative and finite or None."
                )


@dataclass
class Printer:
    id: str
    current_job_id: str | None = None
    busy_time: float = 0.0

    def __post_init__(self) -> None:
        if not isinstance(self.id, str) or not self.id.strip():
            raise ValueError("Printer id must be a non-empty string.")

        if (
            isinstance(self.busy_time, bool)
            or not isinstance(self.busy_time, (int, float))
            or not math.isfinite(self.busy_time)
            or self.busy_time < 0
        ):
            raise ValueError("Busy time must be non-negative and finite.")

        if self.current_job_id is not None and (
            not isinstance(self.current_job_id, str)
            or not self.current_job_id.strip()
        ):
            raise ValueError("Current job ID must be a non-empty string or None.")

    @property
    def is_available(self) -> bool:
        return self.current_job_id is None
