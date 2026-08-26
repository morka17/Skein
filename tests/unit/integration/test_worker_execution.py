"""
Runs worker/worker.py's actual execution loop against real registered
task functions and a fakeredis-backed queue/result store — success,
retry-then-fail, and unregistered-task paths.

This is "integration" in the sense that it exercises worker.py, queue/,
core/, and worker/registry.py together rather than any one in isolation —
not in the sense of needing real infrastructure; fakeredis is enough.
"""

from __future__ import annotations

import asyncio

import pytest

from task_engine.core.states import TaskState
from task_engine.core.task import Task
from task_engine.queue.priority_queue import PriorityQueue
from task_engine.queue.result_store import ResultStore
from task_engine.worker.registry import TaskRegistry
from task_engine.worker.retry import RetryPolicy
from task_engine.worker.worker import run_worker


@pytest.fixture
def registry() -> TaskRegistry:
    """A fresh, isolated registry per test — never touches the shared
    module-level default_registry, so tests can't leak task definitions
    into each other or into a real worker process."""
    reg = TaskRegistry()

    async def add(a: int, b: int) -> int:
        return a + b

    async def always_fails() -> None:
        raise RuntimeError("boom")

    reg.register("add", add)
    reg.register("always_fails", always_fails)
    return reg


async def _drain_queue_briefly(
    queue: PriorityQueue,
    results: ResultStore,
    registry: TaskRegistry,
    retry_policy: RetryPolicy,
    duration: float = 0.3,
) -> None:
    """
    Runs run_worker() for a short, bounded window rather than forever —
    long enough to pop and process whatever's currently in the queue
    (including anything a fast retry re-queues within that window),
    short enough that a test never hangs if something's wrong.
    """
    shutdown_event = asyncio.Event()

    async def stop_after(delay: float) -> None:
        await asyncio.sleep(delay)
        shutdown_event.set()

    stopper = asyncio.create_task(stop_after(duration))
    try:
        await run_worker(
            worker_id="test-worker",
            queue=queue,
            results=results,
            registry=registry,
            retry_policy=retry_policy,
            shutdown_event=shutdown_event,
        )
    finally:
        stopper.cancel()


async def test_successful_task_reaches_success_state_with_its_result(
    queue: PriorityQueue, results: ResultStore, registry: TaskRegistry
) -> None:
    task = Task(name="add", payload={"a": 2, "b": 3})
    task.mark_queued()
    await results.save(task)
    await queue.push(task)

    await _drain_queue_briefly(queue, results, registry, RetryPolicy())

    stored = await results.get(task.id)
    assert stored is not None
    assert stored.state == TaskState.SUCCESS
    assert stored.result == 5


async def test_failing_task_retries_then_eventually_fails_permanently(
    queue: PriorityQueue, results: ResultStore, registry: TaskRegistry
) -> None:
    # Tiny backoff so the retry actually happens within the test window.
    fast_retry = RetryPolicy(base_seconds=0.01, max_seconds=0.05, jitter=False)

    task = Task(name="always_fails", max_retries=1)
    task.mark_queued()
    await results.save(task)
    await queue.push(task)

    # One drain covers the first attempt (-> RETRY, re-queued after
    # backoff) and, since the window outlasts the backoff delay, the
    # second attempt too (-> FAILED, retries exhausted).
    await _drain_queue_briefly(queue, results, registry, fast_retry, duration=0.5)

    stored = await results.get(task.id)
    assert stored is not None
    assert stored.state == TaskState.FAILED
    assert stored.retries == 1
    assert stored.error is not None and "boom" in stored.error


async def test_unregistered_task_name_fails_immediately_without_retrying(
    queue: PriorityQueue, results: ResultStore, registry: TaskRegistry
) -> None:
    task = Task(name="does_not_exist", max_retries=5)
    task.mark_queued()
    await results.save(task)
    await queue.push(task)

    await _drain_queue_briefly(queue, results, registry, RetryPolicy())

    stored = await results.get(task.id)
    assert stored is not None
    assert stored.state == TaskState.FAILED
    # Never even attempted a retry — an unregistered name can't be fixed
    # by waiting, so worker.py fails it on the first attempt.
    assert stored.retries == 0