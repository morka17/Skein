"""
Bridges worker.py's event hook to DAGResolver: whenever a task reaches
SUCCESS, ask the resolver to unlock its DAG's newly-ready children. Also
detects DAG completion and fires an optional callback for it.

This is intentionally the ONLY place that turns a raw task-state-change
event into "go check the DAG" — worker.py itself has zero DAG awareness,
and DAGResolver has zero awareness of *when* to check, only *how*.
"""

from __future__ import annotations

import logging
from typing import Awaitable, Callable, Optional

from task_engine.core.states import TaskState
from task_engine.core.task import Task
from task_engine.queue.dag_resolver import DAGResolver

logger = logging.getLogger(__name__)

DagCompleteHook = Callable[[str], Awaitable[None]]
EventHook = Callable[[Task], Awaitable[None]]


async def _noop_dag_complete(dag_id: str) -> None:
    return None


async def _noop_event(task: Task) -> None:
    return None


class DependencyTracker:
    """
    Usage:
        tracker = DependencyTracker(resolver, on_dag_complete=..., downstream=monitoring.publish)
        pool = WorkerPool(queue, results, on_event=tracker.handle_event)

    `downstream` lets this compose with another EventHook — typically
    monitoring/pubsub.py's publisher, once that module exists — without
    either module importing the other: handle_event() does its DAG work,
    then forwards the same event onward unchanged.
    """

    def __init__(
        self,
        resolver: DAGResolver,
        on_dag_complete: DagCompleteHook = _noop_dag_complete,
        downstream: Optional[EventHook] = None,
    ) -> None:
        self._resolver = resolver
        self._on_dag_complete = on_dag_complete
        self._downstream = downstream or _noop_event

    async def handle_event(self, task: Task) -> None:
        """The EventHook itself — pass this directly as WorkerPool's
        on_event, or via Scheduler.on_event (the usual path)."""
        if task.state == TaskState.SUCCESS and task.dag_id is not None:
            unlocked = await self._resolver.on_task_success(task)
            if unlocked:
                logger.info(
                    "dag %s: task %s unlocked %d child task(s): %s",
                    task.dag_id, task.id, len(unlocked), [t.id for t in unlocked],
                )
            elif await self._resolver.is_complete(task.dag_id):
                # Nothing new to unlock AND every node has succeeded —
                # `task` was the final leaf to finish.
                await self._on_dag_complete(task.dag_id)

        await self._downstream(task)