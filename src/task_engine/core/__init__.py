"""
task_engine.core — pure domain models. No Redis, no FastAPI, no I/O.

Everything downstream (queue, worker, scheduler, api) builds on these
types; nothing in this package imports from those layers.
"""

from task_engine.core.dag import DAG
from task_engine.core.exceptions import (
    CycleDetectedError,
    DagDepthExceededError,
    InvalidPriorityError,
    InvalidTransitionError,
    TaskEngineError,
    UnknownTaskError,
)
from task_engine.core.states import TaskState, is_terminal, validate_transition
from task_engine.core.task import Task

__all__ = [
    "DAG",
    "Task",
    "TaskState",
    "is_terminal",
    "validate_transition",
    "TaskEngineError",
    "InvalidTransitionError",
    "InvalidPriorityError",
    "CycleDetectedError",
    "DagDepthExceededError",
    "UnknownTaskError",
]