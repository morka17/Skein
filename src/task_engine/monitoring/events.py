"""
Event schema for real-time monitoring. Every state change worker.py or
scheduler/dependency_tracker.py makes gets translated into one of these
before publishing — pubsub.py is the transport, this module is the shape
of the data.

Events are their own pydantic models, not raw Task objects, so we control
exactly what crosses the wire to a browser: a Task's full payload can be
large or contain things you don't want broadcast to every WebSocket
subscriber; events carry only what a monitoring UI actually needs.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Literal, Optional, Union

from pydantic import BaseModel, Field

from task_engine.core.states import TaskState
from task_engine.core.task import Task


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


class TaskEvent(BaseModel):
    """One state-change notification for a single task."""

    type: Literal[
        "task.queued",
        "task.running",
        "task.succeeded",
        "task.failed",
        "task.retrying",
        "task.cancelled",
    ]
    task_id: str
    task_name: str
    dag_id: Optional[str] = None
    state: TaskState
    priority: int
    retries: int
    error: Optional[str] = None
    result: Optional[Any] = None
    timestamp: datetime = Field(default_factory=_utcnow)


class DagCompletedEvent(BaseModel):
    """Fired once by scheduler.py when every node in a DAG has succeeded."""

    type: Literal["dag.completed"] = "dag.completed"
    dag_id: str
    timestamp: datetime = Field(default_factory=_utcnow)


Event = Union[TaskEvent, DagCompletedEvent]


_STATE_TO_EVENT_TYPE: dict[TaskState, str] = {
    TaskState.QUEUED: "task.queued",
    TaskState.RUNNING: "task.running",
    TaskState.SUCCESS: "task.succeeded",
    TaskState.FAILED: "task.failed",
    TaskState.RETRY: "task.retrying",
    TaskState.CANCELLED: "task.cancelled",
}


def event_from_task(task: Task) -> TaskEvent:
    """
    Translates a Task's current state into the TaskEvent describing it.
    Raises ValueError for PENDING — a task isn't interesting to a live
    dashboard until it's actually been queued; pubsub.py catches this and
    simply skips publishing rather than treating it as an error.
    """
    event_type = _STATE_TO_EVENT_TYPE.get(task.state)
    if event_type is None:
        raise ValueError(f"no event type defined for task state {task.state}")

    return TaskEvent(
        type=event_type,  # type: ignore[arg-type]
        task_id=task.id,
        task_name=task.name,
        dag_id=task.dag_id,
        state=task.state,
        priority=task.priority,
        retries=task.retries,
        error=task.error,
        # Only attach the result on success — an in-progress or failed
        # task has nothing meaningful here, and omitting it keeps the
        # payload small for the common (non-success) case.
        result=task.result if task.state == TaskState.SUCCESS else None,
    )


def dag_completed_event(dag_id: str) -> DagCompletedEvent:
    return DagCompletedEvent(dag_id=dag_id)