"""
Minimal standalone-task example — no DAG, just a few @task functions
registered on the default registry. scripts/run_worker.py imports this
module before starting a WorkerPool so these names resolve. Submit work
against them once the API and a worker are both running:

    curl -X POST localhost:8000/tasks -H 'content-type: application/json' \\
      -d '{"name": "add", "payload": {"a": 2, "b": 3}}'

    curl -X POST localhost:8000/tasks -H 'content-type: application/json' \\
      -d '{"name": "slow_greet", "payload": {"name": "Ada", "delay_seconds": 1}}'
"""

from __future__ import annotations

import asyncio

from task_engine.worker.registry import task


@task("add")
async def add(a: float, b: float) -> float:
    """The simplest possible task — no I/O, just arithmetic. Good for
    smoke-testing that submission -> queue -> worker -> result actually
    works end to end before wiring up anything real."""
    return a + b


@task("slow_greet")
async def slow_greet(name: str, delay_seconds: float = 1.0) -> str:
    """Stands in for an I/O-bound task (an API call, a DB write) via a
    sleep — worth using this one to watch a task move
    QUEUED -> RUNNING -> SUCCESS in real time over /ws/events instead of
    finishing before you can even open the WebSocket."""
    await asyncio.sleep(delay_seconds)
    return f"Hello, {name}!"


@task("flaky")
async def flaky(fail_times: int = 2) -> str:
    """
    Fails deterministically the first `fail_times` calls, then succeeds —
    demonstrates worker/retry.py's exponential backoff in action. Uses a
    module-level attempt counter, which only makes sense within a single
    worker process; this is here to make retries visible in a demo, not a
    pattern for real tasks — real task retry state should live in the
    task's own payload/external system, and real tasks should be
    idempotent so a retry after a partial failure is actually safe.
    """
    flaky._attempts = getattr(flaky, "_attempts", 0) + 1  # type: ignore[attr-defined]
    if flaky._attempts <= fail_times:  # type: ignore[attr-defined]
        raise RuntimeError(f"simulated failure #{flaky._attempts}")  # type: ignore[attr-defined]
    return "succeeded after retries"