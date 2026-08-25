"""
task_engine.worker — task execution: registry, retry policy, the asyncio
execution loop, and the pool that runs many of them concurrently.

Depends on task_engine.queue (PriorityQueue, ResultStore) and
task_engine.core (Task); nothing in api/ or monitoring/ is imported here —
worker.py exposes an `on_event` hook instead, so those layers plug in
without worker.py depending on them.
"""

from task_engine.worker.pool import WorkerPool
from task_engine.worker.registry import (
    TaskRegistry,
    UnknownRegisteredTaskError,
    default_registry,
    task,
)
from task_engine.worker.retry import RetryPolicy
from task_engine.worker.worker import run_worker

__all__ = [
    "WorkerPool",
    "TaskRegistry",
    "UnknownRegisteredTaskError",
    "default_registry",
    "task",
    "RetryPolicy",
    "run_worker",
]