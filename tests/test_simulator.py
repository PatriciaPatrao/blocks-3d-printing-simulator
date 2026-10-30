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

def test_metrics_include_failed_jobs_that_started():
    simulator = Simulator(printer_count=1)
    job = Job(
        id="J-FAIL",
        material="PLA",
        est_time=10,
        priority=1,
    )
    simulator.add_job(job)

    job.status = JobStatus.FAILED
    job.started_at = job.created_at + 5
    job.finished_at = job.created_at + 8
    simulator._simulation_elapsed = 10
    simulator.printers[0].busy_time = 3

    report = simulator.generate_report()
    metrics = report["metrics"]
    job_report = report["jobs"][0]

    assert job_report["wait_time"] == 5
    assert job_report["run_time"] == 3
    assert metrics["average_wait_time_seconds"] == 5
    assert metrics["median_wait_time_seconds"] == 5
    assert metrics["throughput_jobs_per_second"] == 0.1
    assert metrics["completed_jobs_per_second"] == 0.0
    assert metrics["total_printer_occupancy_seconds"] == 3
    assert metrics["failed_jobs"] == 1
    assert metrics["completed_jobs"] == 0


@pytest.mark.asyncio
async def test_metrics_with_multiple_printers():
    simulator = Simulator(printer_count=2, time_scale=0.5)

    simulator.add_job(Job("J1", "PLA", 0.1, priority=1))
    simulator.add_job(Job("J2", "ABS", 0.1, priority=2))
    simulator.add_job(Job("J3", "PETG", 0.1, priority=3))
    report = await simulator.run()

    metrics = report["metrics"]
    occupancy = metrics["printer_occupancy_seconds"]

    assert metrics["completed_jobs"] == 3
    assert metrics["failed_jobs"] == 0

    occupancy = metrics["printer_occupancy_seconds"]
    assert metrics["total_printer_occupancy_seconds"] == pytest.approx(
        sum(occupancy.values()), abs=0.00001
    )
    assert all(value > 0 for value in occupancy.values())

    elapsed = report["simulation_elapsed_seconds"]
    assert elapsed > 0

    assert metrics["throughput_jobs_per_second"] == pytest.approx(
        3 / elapsed, abs=0.001
    )
    assert metrics["completed_jobs_per_second"] == pytest.approx(
        3 / elapsed, abs=0.001
    )
    assert metrics["failed_jobs"] == 0
    assert metrics["total_printer_occupancy_seconds"] == pytest.approx(
        sum(occupancy.values()), abs=0.00001
    )
    assert all(value > 0 for value in occupancy.values())
    assert metrics["throughput_jobs_per_second"] > 0
    assert metrics["throughput_jobs_per_second"] >= (
        metrics["throughput_jobs_per_second"]
    )


@pytest.mark.asyncio
async def test_second_printer_starts_queued_job_when_first_is_busy():
    simulator = Simulator(printer_count=2, time_scale=0.01)

    long_job = Job("J1", "PLA", 10, priority=1)
    short_job = Job("J3", "ABS", 3, priority=1)
    waiting_job = Job("J2", "PETG", 1, priority=2)

    simulator.add_job(long_job)
    simulator.add_job(short_job)
    simulator.add_job(waiting_job)

    report = await simulator.run()

    assert report["metrics"]["completed_jobs"] == 3
    assert all(
        job["status"] == JobStatus.COMPLETED.value
        for job in report["jobs"]
    )

    assert long_job.started_at is not None
    assert long_job.finished_at is not None
    assert short_job.started_at is not None
    assert short_job.finished_at is not None
    assert waiting_job.started_at is not None

    assert waiting_job.started_at >= short_job.finished_at
    assert waiting_job.started_at < long_job.finished_at


@pytest.mark.asyncio
async def test_simulation_can_only_run_once():
    simulator = Simulator(printer_count=1, time_scale=0.01)
    simulator.add_job(Job("J1", "PLA", 0.01))

    await simulator.run()

    with pytest.raises(
        RuntimeError,
        match="A simulation instance can only run once",
    ):
        await simulator.run()


def test_report_includes_aging_configuration_and_effective_priority():
    simulator = Simulator(
        printer_count=1,
        aging_interval=60,
    )
    job = Job("J1", "PLA", 10, priority=3)
    simulator.add_job(job)

    job.status = JobStatus.RUNNING
    job.started_at = job.created_at + 10
    job.effective_priority_at_start = 2

    report = simulator.generate_report()

    assert report["configuration"]["aging_enabled"] is True
    assert report["configuration"]["aging_interval_seconds"] == 60
    assert report["jobs"][0]["priority"] == 3
    assert report["jobs"][0]["effective_priority_at_start"] == 2


def test_report_shows_aging_disabled_by_default():
    simulator = Simulator(printer_count=1)

    report = simulator.generate_report()

    assert report["configuration"]["aging_enabled"] is False
    assert report["configuration"]["aging_interval_seconds"] is None


def test_running_job_can_be_cancelled_and_printer_continues(
    monkeypatch,
):
    async def scenario():
        simulator = Simulator(printer_count=1, time_scale=1.0)

        first_job = Job(
            id="job-1",
            material="PLA",
            est_time=10,
        )
        second_job = Job(
            id="job-2",
            material="PETG",
            est_time=1,
        )

        simulator.add_job(first_job)
        simulator.add_job(second_job)

        first_job_started = asyncio.Event()

        async def controlled_sleep(delay):
            if delay == 10:
                first_job_started.set()
                await asyncio.Event().wait()

        monkeypatch.setattr(
            "simulator.simulator.asyncio.sleep",
            controlled_sleep,
        )

        run_task = asyncio.create_task(simulator.run())

        await first_job_started.wait()

        assert first_job.status == JobStatus.RUNNING
        assert simulator.cancel_job("job-1") is True

        report = await run_task

        assert first_job.status == JobStatus.CANCELLED
        assert first_job.finished_at is not None

        assert second_job.status == JobStatus.COMPLETED
        assert second_job.finished_at is not None

        assert simulator.printers[0].current_job_id is None
        assert simulator._running_cancellations == {}

        # Validate cancellation reporting and partial execution metrics.
        assert report["metrics"]["cancelled_jobs"] == 1
        assert report["metrics"]["completed_jobs"] == 1
        assert report["metrics"]["total_jobs"] == 2

        cancelled_job_report = next(
            item for item in report["jobs"]
            if item["id"] == "job-1"
        )

        assert cancelled_job_report["status"] == "cancelled"
        assert cancelled_job_report["run_time"] is not None
        assert cancelled_job_report["finished_at"] is not None

        assert (
            report["metrics"]["total_printer_occupancy_seconds"]
            >= cancelled_job_report["run_time"]
        )

    asyncio.run(scenario())


def test_completed_job_cannot_be_cancelled():
    simulator = Simulator(printer_count=1, time_scale=0.001)

    job = Job(
        id="job-1",
        material="PLA",
        est_time=0.01,
    )

    simulator.add_job(job)

    asyncio.run(simulator.run())

    assert job.status == JobStatus.COMPLETED
    assert simulator.cancel_job("job-1") is False