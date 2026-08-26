"""
Tests core/states.py's transition table and core/task.py's mark_*()
methods that enforce it. Pure — no Redis, no asyncio, no fixtures needed.
"""

from __future__ import annotations

import pytest

from task_engine.core.exceptions import InvalidPriorityError, InvalidTransitionError
from task_engine.core.states import TaskState, is_terminal, validate_transition
from task_engine.core.task import Task


def test_new_task_starts_pending() -> None:
    task = Task(name="noop")
    assert task.state == TaskState.PENDING
    assert not task.is_terminal


def test_valid_transition_sequence_updates_state_and_timestamps() -> None:
    task = Task(name="noop")

    task.mark_queued()
    assert task.state == TaskState.QUEUED

    task.mark_running()
    assert task.state == TaskState.RUNNING
    assert task.started_at is not None

    task.mark_success(result=42)
    assert task.state == TaskState.SUCCESS
    assert task.result == 42
    assert task.completed_at is not None
    assert task.is_terminal


@pytest.mark.parametrize(
    "current, illegal_target",
    [
        (TaskState.PENDING, TaskState.SUCCESS),  # can't skip straight to done
        (TaskState.PENDING, TaskState.RUNNING),  # must be QUEUED first
        (TaskState.SUCCESS, TaskState.QUEUED),  # terminal states have no outgoing edges
        (TaskState.FAILED, TaskState.RUNNING),
        (TaskState.CANCELLED, TaskState.SUCCESS),
    ],
)
def test_illegal_transitions_are_rejected(
    current: TaskState, illegal_target: TaskState
) -> None:
    with pytest.raises(InvalidTransitionError):
        validate_transition(current, illegal_target)


def test_mark_failed_sets_error_and_becomes_terminal() -> None:
    task = Task(name="noop")
    task.mark_queued()
    task.mark_running()
    task.mark_failed("boom")

    assert task.state == TaskState.FAILED
    assert task.error == "boom"
    assert task.is_terminal


def test_retry_increments_attempts_and_allows_requeue() -> None:
    task = Task(name="noop", max_retries=2)
    task.mark_queued()
    task.mark_running()
    task.mark_retry("transient error")

    assert task.state == TaskState.RETRY
    assert task.retries == 1
    assert task.can_retry  # 1 < 2

    task.mark_queued()  # RETRY -> QUEUED is a legal transition
    assert task.state == TaskState.QUEUED


def test_can_retry_becomes_false_once_max_retries_exhausted() -> None:
    task = Task(name="noop", max_retries=1)
    task.mark_queued()
    task.mark_running()
    task.mark_retry("err")

    assert task.retries == 1
    assert not task.can_retry


def test_priority_outside_configured_range_is_rejected() -> None:
    with pytest.raises(InvalidPriorityError):
        Task(name="noop", priority=999)


def test_is_terminal_matches_states_with_no_outgoing_transitions() -> None:
    terminal_states = {TaskState.SUCCESS, TaskState.FAILED, TaskState.CANCELLED}
    for state in TaskState:
        assert is_terminal(state) == (state in terminal_states)