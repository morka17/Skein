"""
task_engine/core/states.py

The task state machine. `core/task.py` routes every mutation through
`validate_transition` here, so illegal jumps (e.g. PENDING -> SUCCESS,
skipping execution entirely) fail loudly at the point of the bug instead
of silently corrupting queue/result_store.py or confusing the scheduler.
"""

from __future__ import annotations

from enum import Enum

from task_engine.core.exceptions import InvalidTransitionError


class TaskState(str, Enum):
    PENDING = "PENDING"      # created, not yet eligible (e.g. DAG parents unmet)
    QUEUED = "QUEUED"        # sitting in queue/priority_queue.py, waiting for a worker
    RUNNING = "RUNNING"      # claimed by a worker, executing right now
    SUCCESS = "SUCCESS"      # completed without error — terminal
    FAILED = "FAILED"        # exhausted retries — terminal
    RETRY = "RETRY"          # failed, will be re-queued by worker/retry.py
    CANCELLED = "CANCELLED"  # cancelled before/during execution — terminal


# Allowed transitions: current state -> set of legal next states.
# Anything not listed here is refused by validate_transition().
_TRANSITIONS: dict[TaskState, set[TaskState]] = {
    TaskState.PENDING: {TaskState.QUEUED, TaskState.CANCELLED},
    TaskState.QUEUED: {TaskState.RUNNING, TaskState.CANCELLED},
    TaskState.RUNNING: {
        TaskState.SUCCESS,
        TaskState.FAILED,
        TaskState.RETRY,
        TaskState.CANCELLED,
    },
    TaskState.RETRY: {TaskState.QUEUED, TaskState.FAILED, TaskState.CANCELLED},
    TaskState.SUCCESS: set(),      # terminal
    TaskState.FAILED: set(),       # terminal
    TaskState.CANCELLED: set(),    # terminal
}


def validate_transition(current: TaskState, new: TaskState) -> None:
    """Raises InvalidTransitionError if `current -> new` isn't a legal edge
    in the state machine above. Called by every Task.mark_*() method."""
    if new not in _TRANSITIONS[current]:
        raise InvalidTransitionError(f"cannot transition task from {current} to {new}")


def is_terminal(state: TaskState) -> bool:
    """True for SUCCESS / FAILED / CANCELLED — states with no outgoing edges.
    Used by dag.py to decide when a DAG node is 'done' for dependency purposes."""
    return not _TRANSITIONS[state]