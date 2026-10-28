
import json

from simulator.cli import load_jobs
from simulator.models import Job
from simulator.simulator import Simulator


def test_report_uses_iso_timestamps():
    simulator = Simulator(printer_count=1, time_scale=0.001)
    simulator.add_job(Job("J1", "PLA", 0.01))

    import asyncio
    report = asyncio.run(simulator.run())

    job = report["jobs"][0]
    assert job["created_at"].endswith("+00:00")
    assert job["started_at"].endswith("+00:00")
    assert job["finished_at"].endswith("+00:00")


def test_load_jobs_from_json(tmp_path):
    path = tmp_path / "jobs.json"
    path.write_text(
        json.dumps([
            {"id": "J1", "material": "PLA", "est_time": 10}
        ]),
        encoding="utf-8",
    )

    jobs = load_jobs(path)

    assert len(jobs) == 1
    assert jobs[0].id == "J1"
    assert jobs[0].priority == 0


def test_load_jobs_rejects_invalid_json_structure(tmp_path):
    path = tmp_path / "jobs.json"
    path.write_text('{"unexpected": []}', encoding="utf-8")

    try:
        load_jobs(path)
    except ValueError as exc:
        assert "JSON list" in str(exc)
    else:
        raise AssertionError("Expected invalid input to be rejected")
