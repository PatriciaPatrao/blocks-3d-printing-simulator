import sqlite3

import pytest

from simulator.database import Database
from simulator.models import Job, JobStatus
from simulator.repositories.job_repository import JobRepository


@pytest.fixture
def repository(tmp_path):
    """A test job repository."""
    database = Database(tmp_path / "test_simulator.db")
    return JobRepository(database)


def create_run(repository):
    """Create a run in the repository."""
    return repository.create_run(
        printer_count=2,
        time_scale=0.01,
        aging_interval=None,
        input_file="sample_input.json",
    )


def test_create_run(repository):
    """Test that a run can be created."""
    run_id = create_run(repository)

    result = repository.get_run(run_id)

    assert result is not None
    assert result["run"]["id"] == run_id
    assert result["run"]["status"] == "running"
    assert result["run"]["printer_count"] == 2
    assert result["run"]["time_scale"] == 0.01
    assert result["run"]["input_file"] == "sample_input.json"


def test_create_multiple_runs_with_same_job_id(repository):
    """Test that multiple runs can be created with the same job ID."""
    first_run = create_run(repository)
    second_run = create_run(repository)

    first_job = Job(id="JOB-001", material="PLA", est_time=10)
    second_job = Job(id="JOB-001", material="PETG", est_time=20)

    repository.add_job(first_run, first_job)
    repository.add_job(second_run, second_job)

    first_result = repository.get_run(first_run)
    second_result = repository.get_run(second_run)

    assert first_result["jobs"][0]["material"] == "PLA"
    assert second_result["jobs"][0]["material"] == "PETG"


def test_duplicate_job_id_in_same_run_is_rejected(repository):
    """Test that a duplicate job ID in the same run is rejected."""
    run_id = create_run(repository)

    repository.add_job(
        run_id,
        Job(id="JOB-001", material="PLA", est_time=10),
    )

    with pytest.raises(sqlite3.IntegrityError):
        repository.add_job(
            run_id,
            Job(id="JOB-001", material="PETG", est_time=20),
        )


def test_add_job_records_initial_status(repository):
    """Test that the initial status of a job is recorded."""
    run_id = create_run(repository)
    job = Job(id="JOB-001", material="PLA", est_time=10)

    repository.add_job(run_id, job)

    history = repository.get_job_history(run_id, job.id)

    assert len(history) == 1
    assert history[0]["status"] == JobStatus.QUEUED.value


def test_update_job_records_status_changes(repository):
    """Test that the status changes of a job are recorded."""
    run_id = create_run(repository)
    job = Job(id="JOB-001", material="PLA", est_time=10)

    repository.add_job(run_id, job)

    job.status = JobStatus.RUNNING
    job.started_at = 1000.0
    job.effective_priority_at_start = 0

    repository.update_job(run_id, job)

    job.status = JobStatus.COMPLETED
    job.finished_at = 1010.0
    job.run_time_seconds = 10.0

    repository.update_job(run_id, job)

    history = repository.get_job_history(run_id, job.id)

    assert [entry["status"] for entry in history] == [
        "queued",
        "running",
        "completed",
    ]


def test_update_job_without_status_change_does_not_add_history(repository):
    """Test that a job without a status change does not add history."""
    run_id = create_run(repository)
    job = Job(id="JOB-001", material="PLA", est_time=10)

    repository.add_job(run_id, job)

    job.priority = 2
    repository.update_job(run_id, job)

    history = repository.get_job_history(run_id, job.id)

    assert len(history) == 1


def test_finish_run(repository):
    """Test that a run can be finished."""
    run_id = create_run(repository)

    repository.finish_run(
        run_id=run_id,
        status="completed",
        elapsed_time=12.5,
        metrics={"total_jobs": 3},
    )

    result = repository.get_run(run_id)

    assert result["run"]["status"] == "completed"
    assert result["run"]["elapsed_time"] == 12.5
    assert result["run"]["metrics_json"] == '{"total_jobs": 3}'


def test_finish_run_rejects_invalid_status(repository):
    """Test that a run with an invalid status is rejected."""
    run_id = create_run(repository)

    with pytest.raises(ValueError):
        repository.finish_run(
            run_id=run_id,
            status="cancelled",
            elapsed_time=1.0,
            metrics={},
        )


def test_list_runs_returns_all_runs(repository):
    """Test that all runs are returned."""
    first_run = create_run(repository)
    second_run = create_run(repository)

    runs = repository.list_runs()

    assert len(runs) == 2
    assert {run["id"] for run in runs} == {
        first_run,
        second_run,
    }


def test_get_unknown_run_returns_none(repository):
    """Test that an unknown run returns None."""
    assert repository.get_run("unknown-run") is None


def test_update_job_persists_durations(repository):
    """Test that job durations are persisted."""
    run_id = create_run(repository)
    job = Job(id="JOB-001", material="PLA", est_time=10)

    repository.add_job(run_id, job)

    job.status = JobStatus.RUNNING
    job.started_at = 1000.0
    job.wait_time_seconds = 2.5
    job.effective_priority_at_start = 0

    repository.update_job(run_id, job)

    job.status = JobStatus.COMPLETED
    job.finished_at = 1010.0
    job.run_time_seconds = 10.0

    repository.update_job(run_id, job)

    result = repository.get_run(run_id)
    persisted_job = result["jobs"][0]

    assert persisted_job["wait_time_seconds"] == 2.5
    assert persisted_job["run_time_seconds"] == 10.0


def test_finish_run_records_failure(repository):
    """Test that a failed run stores its error message."""
    run_id = create_run(repository)
    error_message = "RuntimeError: Simulated printer failure"

    repository.finish_run(
        run_id=run_id,
        status="failed",
        elapsed_time=5.0,
        metrics={"total_jobs": 1, "failed_jobs": 1},
        error_message=error_message,
    )

    result = repository.get_run(run_id)

    assert result["run"]["status"] == "failed"
    assert result["run"]["elapsed_time"] == 5.0
    assert result["run"]["error_message"] == error_message
    assert result["run"]["metrics_json"] == (
        '{"total_jobs": 1, "failed_jobs": 1}'
    )


def test_run_data_persists_after_reopening_database(tmp_path):
    """Test that run data remains available after reopening the database."""
    database_path = tmp_path / "test_simulator.db"

    database = Database(database_path)
    repository = JobRepository(database)

    run_id = create_run(repository)
    job = Job(id="JOB-001", material="PLA", est_time=10)

    repository.add_job(run_id, job)

    job.status = JobStatus.RUNNING
    job.started_at = 1000.0
    job.wait_time_seconds = 2.5
    repository.update_job(run_id, job)

    job.status = JobStatus.COMPLETED
    job.finished_at = 1010.0
    job.run_time_seconds = 10.0
    repository.update_job(run_id, job)

    repository.finish_run(
        run_id=run_id,
        status="completed",
        elapsed_time=12.5,
        metrics={"total_jobs": 1},
    )

    reopened_database = Database(database_path)
    reopened_repository = JobRepository(reopened_database)

    result = reopened_repository.get_run(run_id)
    history = reopened_repository.get_job_history(run_id, job.id)

    assert result["run"]["status"] == "completed"
    assert result["jobs"][0]["status"] == "completed"
    assert result["jobs"][0]["wait_time_seconds"] == 2.5
    assert result["jobs"][0]["run_time_seconds"] == 10.0

    assert [entry["status"] for entry in history] == [
        "queued",
        "running",
        "completed",
    ]



import pytest

from simulator.models import Job, JobStatus
from simulator.database import Database
from simulator.repositories.job_repository import JobRepository


def create_active_run(repository):
    """Create an active simulation run."""
    return repository.create_run(
        printer_count=1,
        time_scale=1.0,
        aging_interval=None,
    )


def test_add_job_rejects_finished_run(tmp_path):
    """Test that jobs cannot be added to a finished run."""
    database = Database(tmp_path / "test.db")
    repository = JobRepository(database)

    run_id = create_active_run(repository)
    repository.finish_run(
        run_id=run_id,
        status="completed",
        elapsed_time=1.0,
        metrics={},
    )

    with pytest.raises(ValueError, match="Active simulation run"):
        repository.add_job(
            run_id,
            Job("J1", "PLA", 10),
        )


def test_update_job_rejects_finished_run(tmp_path):
    """Test that jobs cannot be updated after a run finishes."""
    database = Database(tmp_path / "test.db")
    repository = JobRepository(database)

    run_id = create_active_run(repository)
    job = Job("J1", "PLA", 10)
    repository.add_job(run_id, job)

    repository.finish_run(
        run_id=run_id,
        status="completed",
        elapsed_time=1.0,
        metrics={},
    )

    job.status = JobStatus.RUNNING

    with pytest.raises(ValueError, match="Active simulation run"):
        repository.update_job(run_id, job)


def test_add_job_rejects_nonexistent_run(tmp_path):
    """Test that jobs cannot be added to a nonexistent run."""
    database = Database(tmp_path / "test.db")
    repository = JobRepository(database)

    with pytest.raises(ValueError, match="Active simulation run"):
        repository.add_job(
            "nonexistent-run",
            Job("J1", "PLA", 10),
        )



@pytest.mark.parametrize(
    ("initial_status", "new_status"),
    [
        (JobStatus.QUEUED, JobStatus.COMPLETED),
        (JobStatus.RUNNING, JobStatus.QUEUED),
        (JobStatus.COMPLETED, JobStatus.RUNNING),
        (JobStatus.CANCELLED, JobStatus.COMPLETED),
        (JobStatus.FAILED, JobStatus.QUEUED),
    ],
)
def test_update_job_rejects_invalid_status_transitions(
    tmp_path,
    initial_status,
    new_status,
):
    """Test that invalid job status transitions are rejected."""
    database = Database(tmp_path / "test.db")
    repository = JobRepository(database)

    run_id = create_active_run(repository)
    job = Job("J1", "PLA", 10)
    job.status = initial_status

    repository.add_job(run_id, job)

    job.status = new_status

    with pytest.raises(ValueError, match="Invalid job status transition"):
        repository.update_job(run_id, job)

    persisted = repository.get_run(run_id)["jobs"][0]

    assert persisted["status"] == initial_status.value

    history = repository.get_job_history(run_id, "J1")

    assert [entry["status"] for entry in history] == [
        initial_status.value,
    ]


@pytest.mark.parametrize(
    ("initial_status", "new_status"),
    [
        (JobStatus.QUEUED, JobStatus.RUNNING),
        (JobStatus.QUEUED, JobStatus.CANCELLED),
        (JobStatus.QUEUED, JobStatus.FAILED),
        (JobStatus.RUNNING, JobStatus.COMPLETED),
        (JobStatus.RUNNING, JobStatus.CANCELLED),
        (JobStatus.RUNNING, JobStatus.FAILED),
    ],
)
def test_update_job_accepts_valid_status_transitions(
    tmp_path,
    initial_status,
    new_status,
):
    """Test that valid job status transitions are accepted."""
    database = Database(tmp_path / "test.db")
    repository = JobRepository(database)

    run_id = create_active_run(repository)
    job = Job("J1", "PLA", 10)
    job.status = initial_status

    repository.add_job(run_id, job)

    job.status = new_status
    repository.update_job(run_id, job)

    stored_run = repository.get_run(run_id)
    stored_job = next(
        item for item in stored_run["jobs"]
        if item["job_id"] == job.id
    )

    assert stored_job["status"] == new_status.value


def test_update_job_allows_updates_without_status_change(tmp_path):
    """Test that job details can be updated without a status change."""
    database = Database(tmp_path / "test.db")
    repository = JobRepository(database)

    run_id = create_active_run(repository)
    job = Job("J1", "PLA", 10)
    repository.add_job(run_id, job)

    job.priority = 5
    repository.update_job(run_id, job)

    persisted = repository.get_run(run_id)["jobs"][0]

    assert persisted["priority"] == 5

    history = repository.get_job_history(run_id, "J1")

    assert [entry["status"] for entry in history] == [
        "queued",
    ]
