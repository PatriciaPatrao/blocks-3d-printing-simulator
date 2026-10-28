
# 3D Printing Queue Simulator

A Python-based simulator for scheduling 3D printing jobs across multiple printers. It supports priority-based scheduling, FIFO ordering for equal priorities, queued-job cancellation, asynchronous execution and JSON and CSV reporting.

## Features

- Priority scheduling: lower numeric values represent higher priority.
- FIFO ordering for jobs with equal priority.
- Configurable number of printers.
- Asynchronous workers, one per printer.
- Thread-safe queue operations.
- Cancellation of jobs that have not started.
- JSON reports with job states, timestamps and execution metrics.
- CSV reports with per-job states, timestamps, wait times and run times.
- Configurable simulation time scale.
- Automated tests using pytest and pytest-asyncio.

## Requirements

- Python 3.11 or newer.
- pip.

## Installation

Clone or download the repository and open a terminal in the project directory.

Create a virtual environment:

```bash
python -m venv .venv
```

Activate it on Windows Git Bash:

```bash
source .venv/Scripts/activate
```

Install dependencies:

```bash
python -m pip install -r requirements.txt
```

## Input format

Jobs are loaded from a JSON file. The file can contain a list of jobs or an object with a `jobs` array.

Example:

```json
{
  "jobs": [
    {
      "id": "J001",
      "material": "PLA",
      "est_time": 30,
      "priority": 2
    },
    {
      "id": "J002",
      "material": "ABS",
      "est_time": 20,
      "priority": 1
    }
  ]
}
```

Fields:

- `id`: unique job identifier.
- `material`: printing material.
- `est_time`: estimated printing duration in seconds; must be positive.
- `priority`: integer priority, defaulting to `0`. Lower numbers are scheduled first.

The creation timestamp is assigned when a job is loaded.

## Running the simulator

Run the sample workload:

```bash
python -m simulator.cli \
  --input sample_jobs.json \
  --printers 2 \
  --time-scale 0.01 \
  --output report.json
```

Options:

- `--input`: input JSON path; defaults to `sample_jobs.json`.
- `--printers`: number of printers; defaults to `2`.
- `--time-scale`: multiplier applied to estimated job duration; defaults to `0.01`.
- `--output`: report output path. Use `.json` for the full report with metrics or `.csv` for per-job data. If omitted, the JSON report is printed to the terminal.
- `--list`: list loaded jobs before simulation.
- `--run`: run the simulation after listing jobs.
- `--cancel JOB_ID`: cancel a queued job before simulation. This option can be repeated.

List jobs without running the simulation:

```bash
python -m simulator.cli --list
```

Cancel a job before simulation and save the report:

```bash
python -m simulator.cli \
  --cancel J003 \
  --output report_cancel.json
```

Note: each CLI invocation loads a fresh set of jobs from the input file. Job states and cancellations are not persisted between invocations.

## Report

The JSON report contains:

- Per-job status: `queued`, `running`, `completed`, `cancelled` or `failed`.
- UTC timestamps in ISO 8601 format.
- `wait_time`: start time minus creation time, in seconds.
- `run_time`: finish time minus start time, in seconds.
- Average and median waiting times for completed jobs.
- Throughput in completed jobs per second.
- Total printer occupancy and occupancy per printer.

`time_scale` changes simulated duration, not the reported time units. Execution and occupancy durations are measured in real elapsed seconds.

## Tests

Run the test suite:

```bash
python -m pytest -q
```

Tests cover model validation, priority ordering, FIFO behavior, cancellation, simulator execution, report fields and JSON input validation.

## Design decisions

### Priority and FIFO

The queue uses a heap ordered by `(priority, insertion_sequence, job_id)`. The insertion sequence ensures FIFO ordering among jobs with equal priority.

### Concurrency

Each printer has an asynchronous worker. Queue access and job claiming are protected by a lock so that a job cannot be claimed by multiple printers.

### Cancellation

Cancellation is supported only while a job is queued. Running-job cancellation and persistent state are outside the current implementation.

### Time scaling

The time scale makes simulations faster or slower without changing the report's time units. For example, `0.01` makes a nominal 30-second job take approximately 0.3 seconds to execute.

## Limitations

- Job state is held in memory and is reset for each CLI invocation.
- The CLI is not an interactive service.
- Running-job cancellation and preemption are not implemented.
- Printer failures are represented when a worker raises an exception. The affected job is marked as `failed`, the report is preserved, and the CLI exits with a non-zero status. Jobs not yet started remain queued.