import argparse
import asyncio
import json
import csv
import sys
from pathlib import Path

from simulator.models import Job
from simulator.simulator import Simulator


def load_jobs(input_path: Path) -> list[Job]:
    try:
        data = json.loads(input_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(f"Could not read input file: {exc}") from exc

    if isinstance(data, dict):
        data = data.get("jobs")

    if not isinstance(data, list):
        raise ValueError("Input must be a JSON list or an object with 'jobs'.")

    jobs = []
    for index, item in enumerate(data):
        if not isinstance(item, dict):
            raise ValueError(f"Job at index {index} must be a JSON object.")

        try:
            jobs.append(
                Job(
                    id=item["id"],
                    material=item["material"],
                    est_time=item["est_time"],
                    priority=item.get("priority", 0),
                )
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise ValueError(
                f"Invalid job at index {index}: {exc}"
            ) from exc

    return jobs


def write_report(report, output_path):
    output_path = Path(output_path)
    suffix = output_path.suffix.lower()

    if suffix not in {".json", ".csv"}:
        raise ValueError(
            "Unsupported report format. Use a .json or .csv extension."
        )

    output_path.parent.mkdir(parents=True, exist_ok=True)

    if suffix == ".csv":
        fieldnames = [
            "id",
            "material",
            "est_time",
            "priority",
            "effective_priority_at_start",
            "status",
            "created_at",
            "started_at",
            "finished_at",
            "wait_time",
            "run_time",
        ]

        with output_path.open(
            "w", encoding="utf-8", newline=""
        ) as csv_file:
            writer = csv.DictWriter(
                csv_file,
                fieldnames=fieldnames,
                extrasaction="ignore",
            )
            writer.writeheader()
            writer.writerows(report["jobs"])

        return

    with output_path.open("w", encoding="utf-8") as json_file:
        json.dump(report, json_file, indent=2)
        json_file.write("\n")


async def run_cli(args: argparse.Namespace) -> int:
    jobs = load_jobs(args.input)
    simulator = Simulator(
        printer_count=args.printers,
        time_scale=args.time_scale,
        aging_interval=getattr(args, "aging_interval", None),
    )

    for job in jobs:
        simulator.add_job(job)

    if args.add_job is not None:
        if not args.material or args.est_time is None:
            raise ValueError(
                "--add-job requires --material and --est-time."
            )

        simulator.add_job(
            Job(
                id=args.add_job,
                material=args.material,
                est_time=args.est_time,
                priority=args.priority,
            )
        )

    for job_id in args.cancel:
        if not simulator.cancel_job(job_id):
            print(
                f"Warning: job {job_id!r} was not cancelled "
                "(unknown or not queued).",
                file=sys.stderr,
            )

    if args.list:
        for job in simulator.list_jobs():
            print(
                f"{job.id}: status={job.status.value}, "
                f"priority={job.priority}, material={job.material}"
            )

        if not args.run:
            return 0

    simulation_error = None

    try:
        report = await simulator.run()
    except Exception as exc:
        report = simulator.last_report
        simulation_error = exc

    if report is not None:
        output = json.dumps(report, indent=2, ensure_ascii=False)

        if args.output:
            write_report(report, args.output)
            print(f"Report written to {args.output}")
        else:
            print(output)

    if simulation_error is not None:
        print(
            f"Error: Simulation failed: {simulation_error}",
            file=sys.stderr,
        )
        return 1

    return 0



def main() -> int:
    parser = argparse.ArgumentParser(
        description="Simulate a 3D printing job queue."
    )
    parser.add_argument(
        "--input", type=Path, default=Path("sample_input.json"),
        help="Input JSON file (default: sample_input.json)",
    )
    parser.add_argument(
        "--printers", type=int, default=2,
        help="Number of printers (default: 2)",
    )
    parser.add_argument(
        "--time-scale", type=float, default=0.01,
        help="Simulation time multiplier (default: 0.01)",
    )
    parser.add_argument(
        "--output", type=Path,
        help="Report output path (.json or .csv); prints JSON to the terminal if omitted",
    )
    parser.add_argument(
        "--cancel", action="append", default=[], metavar="JOB_ID",
        help="Cancel a queued job; may be specified multiple times",
    )
    parser.add_argument(
        "--list", action="store_true",
        help="List loaded jobs before simulation",
    )
    parser.add_argument(
        "--run", action="store_true",
        help="Run the simulation after listing jobs",
    )
    parser.add_argument(
        "--add-job",
        metavar="JOB_ID",
        help="Add a job to the current simulation.",
    )
    parser.add_argument(
        "--material",
        help="Material for the job being added.",
    )
    parser.add_argument(
        "--est-time",
        type=float,
        help="Estimated printing time in seconds.",
    )
    parser.add_argument(
        "--priority",
        type=int,
        default=0,
        help="Priority for the added job (default: 0; lower is more urgent).",
    )
    parser.add_argument(
        "--aging-interval",
        type=float,
        default=None,
        help=(
            "Enable priority aging with one priority-level improvement "
            "per interval in seconds (disabled by default)."
        ),
    )
    args = parser.parse_args()

    try:
        return asyncio.run(run_cli(args))
    except (ValueError, OSError) as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
