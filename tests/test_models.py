
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

@pytest.mark.parametrize("value", [float("inf"), float("-inf"), float("nan")])
def test_job_rejects_non_finite_estimated_time(value):
    with pytest.raises(ValueError):
        Job(id="J1", material="PLA", est_time=value)

@pytest.mark.parametrize("value", ["invalid", True, float("nan"), float("inf"), -1])
def test_printer_rejects_invalid_busy_time(value):
    with pytest.raises(ValueError):
        Printer(id="P1", busy_time=value)


def test_job_rejects_non_string_id():
    with pytest.raises(ValueError):
        Job(id=None, material="PLA", est_time=10)


def test_printer_rejects_invalid_current_job_id():
    with pytest.raises(ValueError):
        Printer(id="P1", current_job_id="")