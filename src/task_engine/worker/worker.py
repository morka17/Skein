"""
task_engine/worker/worker.py

The core execution loop: pop a task off the priority queue, look up its
function in the registry, run it under a timeout, persist the outcome, and
report it. One `run_worker()` coroutine is one execution "slot" —
worker/pool.py spawns `settings.worker_concurrency` of these concurrently
per process.

Depends on queue.priority_queue.PriorityQueue and queue.result_store.ResultStore
(interfaces pinned down below; implementations live in queue/).
"""

from __future__ import annotations

import asyncio
import logging
from typing import Awaitable, Callable, Optional

from task_engine.config import settings
from task_engine.core.task import Task
from task_engine.queue.priority_queue import PriorityQueue
from task_engine.queue.result_store import ResultStore
from task_engine.worker.registry import (
    TaskRegistry,
    UnknownRegisteredTaskError,
    default_registry,
)
from task_engine.worker.retry import RetryPolicy

logger = logging.getLogger(__name__)

# Fired after every state change (RUNNING, SUCCESS, FAILED, RETRY, QUEUED-
# via-retry) so the monitoring layer can publish it over pub/sub. Kept as
# an injectable callback rather than a hard import of monitoring/pubsub.py
# so worker.py stays testable — and buildable — without that module wired
# up yet.
EventHook = Callable[[Task], Awaitable[None]]


async def _default_hook(task: Task) -> None:
    return None


async def run_worker(
    worker_id: str,
    queue: PriorityQueue,
    results: ResultStore,
    registry: TaskRegistry = default_registry,
    retry_policy: Optional[RetryPolicy] = None,
    on_event: EventHook = _default_hook,
    shutdown_event: Optional[asyncio.Event] = None,
) -> None:
    """
    Runs until `shutdown_event` is set (or forever, if none is given).
    Intended to be one of N coroutines spawned by worker/pool.py, all
    sharing the same `queue` and `results` connections — cheap to run many
    of these per process since they're coroutines, not threads or forks.
    """
    retry_policy = retry_policy or RetryPolicy()
    shutdown_event = shutdown_event or asyncio.Event()

    logger.info("worker %s started", worker_id)

    while not shutdown_event.is_set():
        task = await queue.pop(timeout=settings.worker_poll_interval)
        if task is None:
            continue  # queue empty right now — loop back and poll again

        await _execute(task, registry, results, retry_policy, queue, on_event, worker_id)

    logger.info("worker %s shutting down", worker_id)


async def _execute(
    task: Task,
    registry: TaskRegistry,
    results: ResultStore,
    retry_policy: RetryPolicy,
    queue: PriorityQueue,
    on_event: EventHook,
    worker_id: str,
) -> None:
    task.mark_running()
    await results.save(task)
    await on_event(task)

    try:
        func = registry.get(task.name)
    except UnknownRegisteredTaskError as exc:
        # Not retryable — no amount of waiting fixes a name that was never
        # registered. Fail immediately rather than burning retry budget.
        task.mark_failed(str(exc))
        await results.save(task)
        await on_event(task)
        logger.error("task %s: %s", task.id, exc)
        return

    try:
        result = await asyncio.wait_for(
            func(**task.payload), timeout=settings.task_timeout_seconds
        )
    except asyncio.TimeoutError:
        await _handle_failure(
            task, "task exceeded task_timeout_seconds", retry_policy, queue, results, on_event
        )
    except Exception as exc:  # noqa: BLE001 — task bodies can raise anything; must not crash the worker loop
        logger.exception("task %s (%s) raised", task.id, task.name)
        await _handle_failure(task, repr(exc), retry_policy, queue, results, on_event)
    else:
        task.mark_success(result)
        await results.save(task)
        await on_event(task)
        logger.info("worker %s: task %s (%s) succeeded", worker_id, task.id, task.name)


async def _handle_failure(
    task: Task,
    error: str,
    retry_policy: RetryPolicy,
    queue: PriorityQueue,
    results: ResultStore,
    on_event: EventHook,
) -> None:
    if task.can_retry:
        task.mark_retry(error)
        await results.save(task)
        await on_event(task)
        delay = retry_policy.delay_for(task.retries)
        logger.warning(
            "task %s (%s) failed, retry %d/%d in %.1fs: %s",
            task.id, task.name, task.retries, task.max_retries, delay, error,
        )
        # Scheduled on the event loop rather than awaited inline — this
        # worker slot must go back to polling the queue immediately
        # instead of blocking for the entire backoff delay.
        asyncio.create_task(_requeue_after_delay(task, delay, queue, results, on_event))
    else:
        task.mark_failed(error)
        await results.save(task)
        await on_event(task)
        logger.error("task %s (%s) failed permanently after %d attempts: %s",
                      task.id, task.name, task.retries, error)


async def _requeue_after_delay(
    task: Task,
    delay: float,
    queue: PriorityQueue,
    results: ResultStore,
    on_event: EventHook,
) -> None:
    await asyncio.sleep(delay)
    task.mark_queued()  # RETRY -> QUEUED is a legal transition (core/states.py)
    await results.save(task)
    await queue.push(task)
    await on_event(task)