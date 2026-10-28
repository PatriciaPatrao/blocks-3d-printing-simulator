
import pytest

from simulator.models import Job, JobStatus
from simulator.queue_manager import QueueManager
from concurrent.futures import ThreadPoolExecutor

from threading import Barrier


def test_jobs_are_ordered_by_priority():
    queue = QueueManager()
    queue.enqueue(Job("J1", "PLA", 10, priority=3))
    queue.enqueue(Job("J2", "ABS", 10, priority=1))
    queue.enqueue(Job("J3", "PETG", 10, priority=2))

    assert queue.get_next_job().id == "J2"
    assert queue.get_next_job().id == "J3"
    assert queue.get_next_job().id == "J1"


def test_jobs_with_equal_priority_follow_fifo():
    queue = QueueManager()
    queue.enqueue(Job("J1", "PLA", 10, priority=1))
    queue.enqueue(Job("J2", "ABS", 10, priority=1))
    queue.enqueue(Job("J3", "PETG", 10, priority=1))

    assert queue.get_next_job().id == "J1"
    assert queue.get_next_job().id == "J2"
    assert queue.get_next_job().id == "J3"


def test_cancelled_job_is_skipped():
    queue = QueueManager()
    queue.enqueue(Job("J1", "PLA", 10, priority=1))
    queue.enqueue(Job("J2", "ABS", 10, priority=2))

    assert queue.cancel("J1") is True
    assert queue.get_next_job().id == "J2"
    assert queue.get_next_job() is None


def test_cannot_cancel_unknown_or_already_cancelled_job():
    queue = QueueManager()
    queue.enqueue(Job("J1", "PLA", 10))

    assert queue.cancel("UNKNOWN") is False
    assert queue.cancel("J1") is True
    assert queue.cancel("J1") is False


def test_duplicate_job_ids_are_rejected():
    queue = QueueManager()
    queue.enqueue(Job("J1", "PLA", 10))

    with pytest.raises(ValueError):
        queue.enqueue(Job("J1", "ABS", 20))


def test_queue_count_excludes_cancelled_jobs():
    queue = QueueManager()
    queue.enqueue(Job("J1", "PLA", 10))
    queue.enqueue(Job("J2", "ABS", 10))

    queue.cancel("J1")

    assert queue.queued_count() == 1


def test_concurrent_claims_never_return_the_same_job():
    queue = QueueManager()
    total_jobs = 100
    worker_count = 10

    for i in range(total_jobs):
        queue.enqueue(
            Job(
                id=f"J{i}",
                material="PLA",
                est_time=10,
                priority=1,
            )
        )

    barrier = Barrier(worker_count)

    def claim_jobs():
        claimed = []
        barrier.wait()

        while True:
            job = queue.claim_next_job()
            if job is None:
                break
            claimed.append(job.id)

        return claimed

    with ThreadPoolExecutor(max_workers=worker_count) as executor:
        results = list(executor.map(
            lambda _: claim_jobs(),
            range(worker_count),
        ))

    claimed_ids = [
        job_id
        for worker_result in results
        for job_id in worker_result
    ]

    assert len(claimed_ids) == total_jobs
    assert len(set(claimed_ids)) == total_jobs
    assert all(
        job.status == JobStatus.RUNNING
        for job in queue.list_jobs()
    )


def test_concurrent_cancellation_only_succeeds_once():
    queue = QueueManager()
    queue.enqueue(
        Job(
            id="J1",
            material="PLA",
            est_time=10,
            priority=1,
        )
    )

    worker_count = 10
    barrier = Barrier(worker_count)

    def cancel_job():
        barrier.wait()
        return queue.cancel("J1")

    with ThreadPoolExecutor(max_workers=worker_count) as executor:
        results = list(executor.map(
            lambda _: cancel_job(),
            range(worker_count),
        ))

    assert sum(results) == 1
    assert queue.list_jobs()[0].status == JobStatus.CANCELLED