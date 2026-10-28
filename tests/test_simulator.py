
import pytest

from simulator.models import Job, JobStatus
from simulator.simulator import Simulator


@pytest.mark.asyncio
async def test_simulator_completes_jobs():
    simulator = Simulator(printer_count=2, time_scale=0.001)
    simulator.add_job(Job("J1", "PLA", 0.05))
    simulator.add_job(Job("J2", "ABS", 0.05))

    report = await simulator.run()

    assert report["metrics"]["completed_jobs"] == 2
    assert all(
        job["status"] == JobStatus.COMPLETED.value
        for job in report["jobs"]
    )


@pytest.mark.asyncio
async def test_report_contains_timing_fields():
    simulator = Simulator(printer_count=1, time_scale=0.001)
    simulator.add_job(Job("J1", "PLA", 0.05))

    report = await simulator.run()
    job = report["jobs"][0]

    assert job["started_at"] is not None
    assert job["finished_at"] >= job["started_at"]
    assert job["wait_time"] >= 0
    assert job["run_time"] >= 0


@pytest.mark.asyncio
async def test_cancelled_job_is_not_executed():
    simulator = Simulator(printer_count=1, time_scale=0.001)
    simulator.add_job(Job("J1", "PLA", 0.05))

    assert simulator.cancel_job("J1") is True

    report = await simulator.run()

    assert report["jobs"][0]["status"] == JobStatus.CANCELLED.value
    assert report["metrics"]["completed_jobs"] == 0


def test_simulator_rejects_zero_printers():
    with pytest.raises(ValueError):
        Simulator(printer_count=0)


@pytest.mark.asyncio
async def test_single_printer_respects_priority_and_fifo():
    simulator = Simulator(printer_count=1, time_scale=0.001)

    simulator.add_job(Job("LOW", "PLA", 0.01, priority=3))
    simulator.add_job(Job("HIGH_FIRST", "ABS", 0.01, priority=1))
    simulator.add_job(Job("HIGH_SECOND", "PETG", 0.01, priority=1))
    simulator.add_job(Job("MEDIUM", "PLA", 0.01, priority=2))

    report = await simulator.run()

    jobs = {job["id"]: job for job in report["jobs"]}
    start_order = sorted(
        jobs,
        key=lambda job_id: jobs[job_id]["started_at"],
    )

    assert start_order == [
        "HIGH_FIRST",
        "HIGH_SECOND",
        "MEDIUM",
        "LOW",
    ]
