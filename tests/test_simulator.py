import asyncio

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


@pytest.mark.asyncio
async def test_worker_failure_does_not_leave_job_running(monkeypatch):
    simulator = Simulator(printer_count=1, time_scale=0.01)
    job = Job(
        id="J-FAIL",
        material="PLA",
        est_time=1,
        priority=1,
    )
    simulator.add_job(job)

    async def failing_sleep(_delay):
        raise RuntimeError("Simulated printer failure")

    monkeypatch.setattr(asyncio, "sleep", failing_sleep)

    with pytest.raises(RuntimeError, match="Simulated printer failure"):
        await simulator.run()

    assert job.status == JobStatus.FAILED
    assert job.finished_at is not None
    assert simulator.printers[0].current_job_id is None


@pytest.mark.asyncio
async def test_report_counts_failed_jobs(monkeypatch):
    simulator = Simulator(printer_count=1, time_scale=0.01)
    job = Job(
        id="J-FAIL",
        material="PLA",
        est_time=1,
        priority=1,
    )
    simulator.add_job(job)

    async def failing_sleep(_delay):
        raise RuntimeError("Simulated printer failure")

    monkeypatch.setattr(asyncio, "sleep", failing_sleep)

    with pytest.raises(RuntimeError):
        await simulator.run()

    report = simulator.generate_report()

    assert report["metrics"]["failed_jobs"] == 1
    assert report["metrics"]["completed_jobs"] == 0




@pytest.mark.asyncio
async def test_all_workers_finish_cleanup_when_one_worker_fails(monkeypatch):
    simulator = Simulator(printer_count=2, time_scale=0.01)

    failed_job = Job(
        id="J-FAIL",
        material="PLA",
        est_time=1,
        priority=1,
    )
    other_job = Job(
        id="J-OK",
        material="PETG",
        est_time=1,
        priority=2,
    )
    queued_job = Job(
        id="J-QUEUED",
        material="ABS",
        est_time=1,
        priority=3,
    )

    simulator.add_job(failed_job)
    simulator.add_job(other_job)
    simulator.add_job(queued_job)

    original_sleep = asyncio.sleep
    both_workers_started = asyncio.Event()
    calls = 0

    async def controlled_sleep(delay):
        nonlocal calls
        calls += 1
        current_call = calls

        if calls == 2:
            both_workers_started.set()

        await both_workers_started.wait()

        if current_call == 1:
            raise RuntimeError("Simulated printer failure")

        await original_sleep(delay)

    monkeypatch.setattr(asyncio, "sleep", controlled_sleep)

    with pytest.raises(RuntimeError, match="Simulated printer failure"):
        await simulator.run()

    assert failed_job.status == JobStatus.FAILED
    assert failed_job.finished_at is not None

    assert other_job.status == JobStatus.COMPLETED
    assert other_job.finished_at is not None

    assert queued_job.status == JobStatus.QUEUED
    assert all(printer.current_job_id is None for printer in simulator.printers)


@pytest.mark.asyncio
async def test_report_remains_available_after_worker_failure(monkeypatch):
    simulator = Simulator(printer_count=1, time_scale=0.01)
    job = Job(
        id="J-FAIL",
        material="PLA",
        est_time=1,
        priority=1,
    )
    simulator.add_job(job)

    async def failing_sleep(_delay):
        raise RuntimeError("Simulated printer failure")

    monkeypatch.setattr(asyncio, "sleep", failing_sleep)

    with pytest.raises(RuntimeError, match="Simulated printer failure"):
        await simulator.run()

    assert simulator.last_report is not None
    assert simulator.last_report["metrics"]["failed_jobs"] == 1
    assert simulator.last_report["metrics"]["completed_jobs"] == 0
    assert simulator.last_report["jobs"][0]["status"] == "failed"
