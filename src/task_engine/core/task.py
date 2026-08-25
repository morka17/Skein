"""
task_engine/core/task.py

The Task domain model — the unit of work the entire engine revolves around.
Pure data + state-machine behavior, no I/O: nothing here touches Redis, the
network, or the filesystem. `queue/result_store.py` is responsible for
serializing/persisting instances of this class; `worker/worker.py` is
responsible for actually invoking the function a Task references.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Any, Optional

from pydantic import BaseModel, Field, field_validator

from task_engine.config import settings
from task_engine.core.exceptions import InvalidPriorityError
from task_engine.core.states import TaskState, validate_transition


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


class Task(BaseModel):
    """
    A single unit of work.

    `name` must correspond to a function registered via `@task(...)` in
    `worker/registry.py` — the Task itself only ever carries the *reference*
    and the *payload*, never the callable, so it can be serialized to Redis
    (queue/result_store.py) without pickling code.
    """

    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    name: str = Field(..., description="Registered task name — a worker/registry.py key")
    payload: dict[str, Any] = Field(default_factory=dict)

    priority: int = Field(default_factory=lambda: settings.default_priority)
    state: TaskState = Field(default=TaskState.PENDING)

    dag_id: Optional[str] = Field(
        default=None, description="Set by DAG.from_tasks() when this is a node in a graph"
    )
    depends_on: list[str] = Field(
        default_factory=list,
        description="IDs of parent tasks that must reach SUCCESS before this can run",
    )

    retries: int = Field(default=0, description="Number of attempts made so far")
    max_retries: int = Field(default_factory=lambda: settings.max_retries)

    result: Optional[Any] = Field(default=None)
    error: Optional[str] = Field(default=None)

    created_at: datetime = Field(default_factory=_utcnow)
    updated_at: datetime = Field(default_factory=_utcnow)
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None

    @field_validator("priority")
    @classmethod
    def _priority_in_range(cls, v: int) -> int:
        if not (settings.min_priority <= v <= settings.max_priority):
            raise InvalidPriorityError(
                f"priority {v} out of range [{settings.min_priority}, {settings.max_priority}]"
            )
        return v

    # ------------------------------------------------------------------
    # State transitions — the ONLY sanctioned way `state` changes. Never
    # set `task.state = ...` directly anywhere in the codebase; route
    # through these so states.py's transition table is enforced uniformly
    # across worker.py, scheduler.py, and retry.py.
    # ------------------------------------------------------------------

    def _transition(self, new_state: TaskState) -> None:
        validate_transition(self.state, new_state)
        self.state = new_state
        self.updated_at = _utcnow()

    def mark_queued(self) -> None:
        """Called by dag_resolver.py / priority_queue.py once a task is pushed."""
        self._transition(TaskState.QUEUED)

    def mark_running(self) -> None:
        """Called by worker.py the instant a worker claims the task."""
        self._transition(TaskState.RUNNING)
        self.started_at = _utcnow()

    def mark_success(self, result: Any = None) -> None:
        self._transition(TaskState.SUCCESS)
        self.result = result
        self.completed_at = _utcnow()

    def mark_failed(self, error: str) -> None:
        self._transition(TaskState.FAILED)
        self.error = error
        self.completed_at = _utcnow()

    def mark_retry(self, error: str) -> None:
        """Called by worker/retry.py on a recoverable failure — bumps the
        attempt count; the caller is responsible for re-queuing afterward
        (typically via mark_queued() once the backoff delay elapses)."""
        self._transition(TaskState.RETRY)
        self.error = error
        self.retries += 1

    def mark_cancelled(self) -> None:
        self._transition(TaskState.CANCELLED)
        self.completed_at = _utcnow()

    @property
    def can_retry(self) -> bool:
        return self.retries < self.max_retries

    @property
    def is_terminal(self) -> bool:
        return self.state in (TaskState.SUCCESS, TaskState.FAILED, TaskState.CANCELLED)