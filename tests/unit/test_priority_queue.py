"""
Tests queue/priority_queue.py: push/pop round-tripping, priority
ordering, FIFO tie-breaking within a priority level, and cancellation.
Runs against fakeredis via the `queue` fixture in conftest.py.
"""

from __future__ import annotations

import asyncio

from task_engine.core.task import Task
from task_engine.queue.priority_queue import PriorityQueue


async def test_push_then_pop_returns_the_same_task(queue: PriorityQueue) -> None:
    task = Task(name="noop", payload={"x": 1})
    task.mark_queued()
    await queue.push(task)

    popped = await queue.pop(timeout=1)

    assert popped is not None
    assert popped.id == task.id
    assert popped.payload == {"x": 1}


async def test_pop_on_empty_queue_returns_none_after_timeout(queue: PriorityQueue) -> None:
    popped = await queue.pop(timeout=0.2)
    assert popped is None


async def test_higher_priority_pops_first_regardless_of_push_order(
    queue: PriorityQueue,
) -> None:
    urgent = Task(name="noop", priority=0)
    normal = Task(name="noop", priority=5)
    urgent.mark_queued()
    normal.mark_queued()

    # Push the LOWER-priority (less urgent) task first — if ordering were
    # FIFO-only, `normal` would pop first. It shouldn't.
    await queue.push(normal)
    await queue.push(urgent)

    first = await queue.pop(timeout=1)
    second = await queue.pop(timeout=1)

    assert first.id == urgent.id
    assert second.id == normal.id


async def test_same_priority_pops_in_fifo_order(queue: PriorityQueue) -> None:
    first_task = Task(name="noop", priority=5)
    first_task.mark_queued()
    await queue.push(first_task)

    await asyncio.sleep(0.01)  # ensure a distinctly later timestamp

    second_task = Task(name="noop", priority=5)
    second_task.mark_queued()
    await queue.push(second_task)

    popped_first = await queue.pop(timeout=1)
    popped_second = await queue.pop(timeout=1)

    assert popped_first.id == first_task.id
    assert popped_second.id == second_task.id


async def test_size_reflects_current_queue_depth(queue: PriorityQueue) -> None:
    assert await queue.size() == 0

    task = Task(name="noop")
    task.mark_queued()
    await queue.push(task)
    assert await queue.size() == 1

    await queue.pop(timeout=1)
    assert await queue.size() == 0


async def test_remove_cancels_a_still_queued_task(queue: PriorityQueue) -> None:
    task = Task(name="noop")
    task.mark_queued()
    await queue.push(task)

    removed = await queue.remove(task.id)
    assert removed is True
    assert await queue.size() == 0

    removed_again = await queue.remove(task.id)
    assert removed_again is False  # already gone, nothing left to remove