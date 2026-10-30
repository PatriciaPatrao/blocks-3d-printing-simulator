
# 3D Printing Queue Simulator

A Python-based simulator for scheduling 3D printing jobs across multiple printers. It supports priority-based scheduling, FIFO ordering for equal priorities, queued and running-job cancellation, asynchronous execution and JSON and CSV reporting.

## Features

- Priority scheduling: lower numeric values represent higher priority.
- FIFO ordering for jobs with equal priority.
- Configurable number of printers.
- Asynchronous workers, one per printer.
- Thread-safe queue operations.
- Cancellation of queued and running jobs through the Python API.
- CLI support for cancelling queued jobs before simulation.
- JSON reports with job states, timestamps and execution metrics.
- CSV reports with per-job states, timestamps, wait times and run times.
- Configurable simulation time scale.
- Automated tests using pytest and pytest-asyncio.
- Optional Priority Aging to reduce starvation of lower-priority jobs.
- Configurable aging interval, disabled by default.

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
  --input sample_input.json \
  --printers 2 \
  --output report.json
```
The simulator runs the workload using two printers and saves the full report, including job states and metrics, to report.json.

Add a job to the workload for the current invocation:

```bash
python -m simulator.cli \
  --add-job J005 \
  --material PLA \
  --est-time 30 \
  --priority 1 \
  --output report.json
```
The new job is added to the jobs loaded from the input file. The simulation then runs unless --list is used without --run.

## Options:

- `--input`: input JSON path; defaults to `sample_input.json`.
- `--printers`: number of printers; defaults to `2`.
- `--time-scale`: multiplier applied to estimated job duration; defaults to `0.01`.
- `--output`: report output path. Use `.json` for the full report with metrics or `.csv` for per-job data. If omitted, the JSON report is printed to the terminal.
- `--add-job JOB_ID`: add a job to the current simulation.
- `--material MATERIAL`: material for the job being added; required with `--add-job`.
- `--est-time SECONDS`: estimated printing time in seconds; required with `--add-job`.
- `--priority PRIORITY`: priority for the added job; defaults to `0`. Lower numbers represent higher priority.
- `--aging-interval SECONDS`: enable Priority Aging, improving a queued job's effective priority by one level for each elapsed interval. Must be positive. Disabled by default.
- `--cancel JOB_ID`: cancel a queued job before simulation. This option can be repeated.
- `--list`: list loaded jobs before simulation.
- `--run`: run the simulation after listing jobs.

List jobs without running the simulation:

```bash
python -m simulator.cli --list
```

Enable Priority Aging with a 60-second interval:

```bash
python -m simulator.cli \
  --input sample_input.json \
  --printers 2 \
  --aging-interval 60 \
  --output report.json
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
- `configuration`: records whether Priority Aging is enabled and its configured interval.
- `priority`: the job's original priority, which remains unchanged.
- `effective_priority_at_start`: the effective priority when the job was claimed by a printer. This is `null` for jobs that have not started.
- `wait_time`: start time minus creation time, in seconds.
- `run_time`: finish time minus start time, in seconds.
- Average and median waiting times for jobs that started, including failed jobs.
- Throughput: total submitted jobs divided by total simulation elapsed time, as specified by the challenge.
- Completed-job rate: completed jobs divided by total simulation elapsed time, measuring effective output.
- Total printer occupancy and occupancy per printer.

`time_scale` changes simulated duration, not the reported time units. Execution and occupancy durations are measured in real elapsed seconds.

## Tests

Run the test suite:

```bash
python -m pytest -q
```

Tests cover model validation, priority ordering, FIFO behavior, Priority Aging, cancellation, simulator execution, report fields and JSON input validation.

## Design decisions

### Priority and FIFO

Jobs are stored in a min-heap using (priority, insertion_sequence, job_id) as the ordering key. Lower numeric priority values are processed first. The insertion sequence preserves FIFO ordering for jobs with equal priority, without relying on job identifiers to determine their order.

This approach provides efficient insertion and retrieval of the next eligible job. Duplicate job identifiers are rejected to prevent ambiguous job state.

### Priority Aging

Priority Aging is an optional scheduling mechanism designed to reduce starvation, where lower-priority jobs may otherwise wait indefinitely while higher-priority jobs continue to arrive.

The effective priority is calculated as:

`effective_priority = max(0, original_priority - floor(waiting_time / aging_interval))`

Each elapsed aging interval improves the job's priority by one level. Priority values cannot become negative.

The original priority is preserved. Aging only affects jobs that remain queued and does not change the priority of jobs that have already started.

Because priority values change over time, the queue is rebuilt before each job claim when aging is enabled. The existing insertion sequence is preserved, maintaining FIFO ordering between jobs with equal effective priority.

Queue waiting time for aging calculations uses a monotonic clock to avoid changes caused by system clock adjustments.

The implementation uses an O(n) heap rebuild per claim when aging is enabled. This is a deliberate trade-off that keeps the scheduling logic simple and avoids background update mechanisms. When aging is disabled, the original heap-based scheduling behavior is preserved.

### Concurrency

Each printer has an asynchronous worker. Queue access and job claiming are protected by a lock so that a job cannot be claimed by multiple printers.

### Cancellation and thread safety

The simulator uses one asynchronous worker per printer. Workers claim jobs through the shared queue manager, which protects queue operations and job claiming with a threading.Lock.

Job claiming and the transition from queued to running happen atomically. This prevents multiple workers from claiming the same job and keeps cancellation from succeeding after a job has started.

### Cancellation and job lifecycle

Queued jobs can be cancelled before execution through the queue manager or the CLI.

Running jobs can also be cancelled through the `Simulator.cancel_job(job_id)` Python API. The cancellation request interrupts the simulated printing wait without terminating the printer worker. The printer is then available to process the next queued job.

The method returns `True` when a cancellation request is accepted and `False` when the job does not exist or cannot be cancelled, including jobs that have already started and received a cancellation request, or have already finished.

Cancelled jobs retain their start and finish timestamps when applicable. Their partial execution time and printer occupancy are included in the report.

Job state is kept in memory rather than persisted between CLI invocations.

### Time scaling and reporting

The time_scale parameter controls the actual duration of simulated printing. It allows longer workloads to be tested quickly without changing the reported time units.

The report includes UTC timestamps, per-job waiting and execution times, job-state counts, waiting-time statistics, throughput, completed-job rate and printer occupancy. Execution and occupancy durations are measured using real elapsed time, so results may vary slightly between runs.

### Trade-offs and scope

The implementation prioritises a small, testable simulation core over additional infrastructure. It uses in-memory state, a heap-based priority queue and asynchronous workers rather than introducing a database, distributed queue or web service.

This keeps the project straightforward to install and run while demonstrating priority scheduling, concurrent execution, cancellation, error handling and structured reporting. Persistence, preemption and an interactive API remain outside the current scope. Running-job cancellation is available through the internal Python API, but not through the CLI.

## Limitations

- Job state is held in memory and is reset for each CLI invocation.
- The CLI is not an interactive service.
- Running-job cancellation is available through the Python API but not through the CLI.
- Preemption is not implemented.