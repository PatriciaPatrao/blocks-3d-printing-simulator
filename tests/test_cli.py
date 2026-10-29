import pytest
import asyncio
from argparse import Namespace
import json
import csv


from simulator import cli

from simulator.cli import load_jobs
from simulator.cli import write_report
from simulator.models import Job
from simulator.simulator import Simulator


def test_report_uses_iso_timestamps():
    simulator = Simulator(printer_count=1, time_scale=0.001)
    simulator.add_job(Job("J1", "PLA", 0.01))

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


def test_cli_saves_report_when_simulation_fails(tmp_path, monkeypatch, capsys):
    input_path = tmp_path / "jobs.json"
    input_path.write_text(
        json.dumps([
            {"id": "J1", "material": "PLA", "est_time": 1}
        ]),
        encoding="utf-8",
    )

    output_path = tmp_path / "report.json"

    expected_report = {
        "jobs": [{"id": "J1", "status": "failed"}],
        "metrics": {"failed_jobs": 1},
    }

    class FailingSimulator:
        def __init__(self, printer_count, time_scale):
            self.last_report = expected_report

        def add_job(self, job):
            pass

        def cancel_job(self, job_id):
            return False

        def list_jobs(self):
            return []

        async def run(self):
            raise RuntimeError("Simulated printer failure")

    monkeypatch.setattr(cli, "Simulator", FailingSimulator)

    args = Namespace(
        input=input_path,
        printers=2,
        time_scale=0.01,
        output=output_path,
        cancel=[],
        list=False,
        run=False,
        add_job=None,
        material=None,
        est_time=None,
        priority=0,
    )

    exit_code = asyncio.run(cli.run_cli(args))

    assert exit_code == 1
    assert json.loads(output_path.read_text(encoding="utf-8")) == expected_report

    captured = capsys.readouterr()
    assert "Report written to" in captured.out
    assert "Simulation failed: Simulated printer failure" in captured.err


def test_write_report_as_csv(tmp_path):
    output_path = tmp_path / "reports" / "report.csv"
    report = {
        "jobs": [
            {
                "id": "J1",
                "material": "PLA",
                "est_time": 10,
                "priority": 1,
                "status": "completed",
                "created_at": "2026-10-28T10:00:00+00:00",
                "started_at": "2026-10-28T10:00:01+00:00",
                "finished_at": "2026-10-28T10:00:11+00:00",
                "wait_time": 1.0,
                "run_time": 10.0,
            }
        ],
        "metrics": {"completed_jobs": 1},
    }

    write_report(report, output_path)

    with output_path.open(
        "r", encoding="utf-8", newline=""
    ) as csv_file:
        rows = list(csv.DictReader(csv_file))

    assert len(rows) == 1
    assert rows[0]["id"] == "J1"
    assert rows[0]["status"] == "completed"
    assert rows[0]["wait_time"] == "1.0"
    assert rows[0]["run_time"] == "10.0"


def test_write_report_rejects_unsupported_extension(tmp_path):
    output_path = tmp_path / "report.txt"
    report = {"jobs": [], "metrics": {}}

    with pytest.raises(ValueError, match="Unsupported report format"):
        write_report(report, output_path)

    assert not output_path.exists()


def test_cli_adds_job_to_simulation(tmp_path):
    input_path = tmp_path / "jobs.json"
    input_path.write_text("[]", encoding="utf-8")
    output_path = tmp_path / "report.json"

    args = Namespace(
        input=input_path,
        printers=1,
        time_scale=0.001,
        output=output_path,
        cancel=[],
        list=False,
        run=False,
        add_job="J005",
        material="PLA",
        est_time=0.01,
        priority=1,
    )

    exit_code = asyncio.run(cli.run_cli(args))

    assert exit_code == 0

    report = json.loads(output_path.read_text(encoding="utf-8"))
    assert len(report["jobs"]) == 1
    assert report["jobs"][0]["id"] == "J005"
    assert report["jobs"][0]["material"] == "PLA"
    assert report["jobs"][0]["priority"] == 1
    assert report["jobs"][0]["status"] == "completed"