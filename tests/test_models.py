
import pytest

from simulator.models import Job, JobStatus, Printer


def test_job_uses_default_values():
    job = Job(id="J1", material="PLA", est_time=10, priority=1)

    assert job.status == JobStatus.QUEUED
    assert job.started_at is None
    assert job.finished_at is None
    assert job.created_at > 0


def test_job_rejects_non_positive_estimated_time():
    with pytest.raises(ValueError):
        Job(id="J1", material="PLA", est_time=0)


def test_job_rejects_non_integer_priority():
    with pytest.raises(ValueError):
        Job(id="J1", material="PLA", est_time=10, priority=1.5)


def test_printer_is_available_when_idle():
    printer = Printer(id="P1")

    assert printer.is_available is True


def test_printer_is_unavailable_when_running_a_job():
    printer = Printer(id="P1", current_job_id="J1")

    assert printer.is_available is False
