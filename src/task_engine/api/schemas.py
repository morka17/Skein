"""
Pydantic request/response models for the HTTP surface. Deliberately kept
separate from core.Task / core.DAG — those are internal domain models;
these are the public contract. Keeping them apart means core.Task can grow
internal-only fields later without silently changing the API response
shape, and the API can evolve its own versioning independent of the
engine's internals.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Optional

from pydantic import BaseModel, Field

from task_engine.core.states import TaskState
from task_engine.core.task import Task

# ------------------------------------------------------------------
# Tasks
# ------------------------------------------------------------------


class TaskSubmitRequest(BaseModel):
    name: str = Field(..., description="Registered task name — see worker/registry.py")
    payload: dict[str, Any] = Field(default_factory=dict)
    priority: Optional[int] = Field(
        default=None,
        description="Lower = more urgent. Omit to use settings.default_priority.",
    )
    max_retries: Optional[int] = Field(
        default=None, description="Omit to use settings.max_retries."
    )


class TaskResponse(BaseModel):
    id: str
    name: str
    payload: dict[str, Any]
    priority: int
    state: TaskState
    dag_id: Optional[str]
    depends_on: list[str]
    retries: int
    max_retries: int
    result: Optional[Any]
    error: Optional[str]
    created_at: datetime
    updated_at: datetime
    started_at: Optional[datetime]
    completed_at: Optional[datetime]

    @classmethod
    def from_task(cls, task: Task) -> "TaskResponse":
        return cls(**task.model_dump())


# ------------------------------------------------------------------
# DAGs
# ------------------------------------------------------------------


class DagNodeSpec(BaseModel):
    """
    One node in a submitted DAG. `id` is client-chosen (not server-
    generated, unlike a standalone task submission) specifically so edges
    can reference nodes by a human-readable name in the same request —
    e.g. id="fetch_data" rather than a UUID the client hasn't seen yet.
    """

    id: str = Field(..., description="Client-chosen unique id, referenced by edges")
    name: str = Field(..., description="Registered task name")
    payload: dict[str, Any] = Field(default_factory=dict)
    priority: Optional[int] = None
    max_retries: Optional[int] = None


class DagSubmitRequest(BaseModel):
    nodes: list[DagNodeSpec]
    edges: list[tuple[str, str]] = Field(
        default_factory=list,
        description="(parent_node_id, child_node_id) pairs, referencing DagNodeSpec.id values",
    )


class DagSubmitResponse(BaseModel):
    dag_id: str
    task_ids: list[str] = Field(description="Every node id in the DAG, as submitted")
    queued: list[str] = Field(description="Root node ids (no parents) queued immediately")


class DagStatusResponse(BaseModel):
    dag_id: str
    complete: bool
    tasks: list[TaskResponse]