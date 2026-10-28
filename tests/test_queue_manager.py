
import pytest

from simulator.models import Job, JobStatus
from simulator.queue_manager import QueueManager


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
