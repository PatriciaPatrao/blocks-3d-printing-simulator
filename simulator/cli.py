
import argparse
import asyncio
import json
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


async def run_cli(args: argparse.Namespace) -> int:
    jobs = load_jobs(args.input)
    simulator = Simulator(
        printer_count=args.printers,
        time_scale=args.time_scale,
    )

    for job in jobs:
        simulator.add_job(job)

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

    report = await simulator.run()
    output = json.dumps(report, indent=2, ensure_ascii=False)

    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(output + "\n", encoding="utf-8")
        print(f"Report written to {args.output}")
    else:
        print(output)

    return 0


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Simulate a 3D printing job queue."
    )
    parser.add_argument(
        "--input", type=Path, default=Path("sample_jobs.json"),
        help="Input JSON file (default: sample_jobs.json)",
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
        help="Report output path; prints JSON to the terminal if omitted",
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

    args = parser.parse_args()

    try:
        return asyncio.run(run_cli(args))
    except (ValueError, OSError) as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
