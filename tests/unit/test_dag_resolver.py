"""
Two things under test:
  - core/dag.py's pure graph algorithms (cycle detection, topological
    order, readiness) — no I/O involved at all.
  - queue/dag_resolver.py's orchestration of those algorithms against
    live Redis state (fakeredis via the `resolver`/`results`/`queue`
    fixtures) — submission, fan-out unlocking, fan-in gating, and the
    idempotency that makes the reconciliation safety-net loop safe.
"""

from __future__ import annotations

import pytest

from task_engine.core.dag import DAG
from task_engine.core.exceptions import (
    CycleDetectedError,
    DagDepthExceededError,
    UnknownTaskError,
)
from task_engine.core.states import TaskState
from task_engine.core.task import Task
from task_engine.queue.dag_resolver import DAGResolver
from task_engine.queue.priority_queue import PriorityQueue
from task_engine.queue.result_store import ResultStore

# ------------------------------------------------------------------
# core.DAG — pure graph algorithms, no fixtures needed
# ------------------------------------------------------------------


def test_linear_dag_topological_order() -> None:
    tasks = [Task(id="a", name="noop"), Task(id="b", name="noop"), Task(id="c", name="noop")]
    dag = DAG.from_tasks(tasks, [("a", "b"), ("b", "c")])
    assert dag.topological_order() == ["a", "b", "c"]


def test_root_and_leaf_nodes() -> None:
    tasks = [Task(id=i, name="noop") for i in ("a", "b", "c")]
    dag = DAG.from_tasks(tasks, [("a", "b"), ("a", "c")])
    assert dag.root_nodes() == ["a"]
    assert set(dag.leaf_nodes()) == {"b", "c"}


def test_cycle_is_rejected_at_construction() -> None:
    tasks = [Task(id=i, name="noop") for i in ("a", "b")]
    with pytest.raises(CycleDetectedError):
        DAG.from_tasks(tasks, [("a", "b"), ("b", "a")])


def test_unknown_edge_endpoint_is_rejected() -> None:
    tasks = [Task(id="a", name="noop")]
    with pytest.raises(UnknownTaskError):
        DAG.from_tasks(tasks, [("a", "nonexistent")])


def test_depth_limit_is_enforced(monkeypatch: pytest.MonkeyPatch) -> None:
    from task_engine.config import settings

    monkeypatch.setattr(settings, "max_dag_depth", 2)
    tasks = [Task(id=str(i), name="noop") for i in range(3)]
    edges = [(str(i), str(i + 1)) for i in range(2)]  # a 3-node chain = depth 3

    with pytest.raises(DagDepthExceededError):
        DAG.from_tasks(tasks, edges)


def test_ready_nodes_requires_every_parent_to_have_succeeded() -> None:
    tasks = [Task(id=i, name="noop") for i in ("a", "b", "c")]
    dag = DAG.from_tasks(tasks, [("a", "c"), ("b", "c")])  # fan-in on c

    assert set(dag.ready_nodes(set())) == {"a", "b"}
    assert dag.ready_nodes({"a"}) == []  # b hasn't succeeded yet
    assert dag.ready_nodes({"a", "b"}) == ["c"]


def test_is_complete_true_only_once_every_node_has_succeeded() -> None:
    tasks = [Task(id=i, name="noop") for i in ("a", "b")]
    dag = DAG.from_tasks(tasks, [("a", "b")])
    assert not dag.is_complete({"a"})
    assert dag.is_complete({"a", "b"})


# ------------------------------------------------------------------
# queue.DAGResolver — Redis-backed orchestration (fakeredis)
# ------------------------------------------------------------------


async def test_submit_queues_only_root_nodes(
    resolver: DAGResolver, results: ResultStore, queue: PriorityQueue
) -> None:
    tasks = [Task(id=i, name="noop") for i in ("a", "b", "c")]
    dag = DAG.from_tasks(tasks, [("a", "b"), ("a", "c")])

    queued = await resolver.submit(dag)

    assert [t.id for t in queued] == ["a"]
    assert await queue.size() == 1

    stored_b = await results.get("b")
    assert stored_b is not None
    assert stored_b.state == TaskState.PENDING


async def test_on_task_success_unlocks_fan_out_children(
    resolver: DAGResolver, results: ResultStore, queue: PriorityQueue
) -> None:
    tasks = [Task(id=i, name="noop") for i in ("a", "b", "c")]
    dag = DAG.from_tasks(tasks, [("a", "b"), ("a", "c")])
    await resolver.submit(dag)

    task_a = await results.get("a")
    assert task_a is not None
    task_a.mark_running()
    task_a.mark_success(result=None)
    await results.save(task_a)

    unlocked = await resolver.on_task_success(task_a)

    assert {t.id for t in unlocked} == {"b", "c"}
    assert await queue.size() == 2  # both b and c queued from one event


async def test_on_task_success_gates_on_all_parents_fan_in(
    resolver: DAGResolver, results: ResultStore, queue: PriorityQueue
) -> None:
    tasks = [Task(id=i, name="noop") for i in ("a", "b", "c")]
    dag = DAG.from_tasks(tasks, [("a", "c"), ("b", "c")])
    await resolver.submit(dag)  # a and b are both roots, both queued

    task_a = await results.get("a")
    task_a.mark_running()
    task_a.mark_success(result=None)
    await results.save(task_a)

    unlocked = await resolver.on_task_success(task_a)
    assert unlocked == []  # c still waiting on b

    task_b = await results.get("b")
    task_b.mark_running()
    task_b.mark_success(result=None)
    await results.save(task_b)

    unlocked = await resolver.on_task_success(task_b)
    assert [t.id for t in unlocked] == ["c"]


async def test_resweep_is_idempotent(
    resolver: DAGResolver, results: ResultStore, queue: PriorityQueue
) -> None:
    """This is the property the reconciliation loop (scheduler.py) leans
    on entirely: calling resweep() redundantly must never double-queue a
    node."""
    tasks = [Task(id=i, name="noop") for i in ("a", "b")]
    dag = DAG.from_tasks(tasks, [("a", "b")])
    await resolver.submit(dag)

    task_a = await results.get("a")
    task_a.mark_running()
    task_a.mark_success(result=None)
    await results.save(task_a)

    first_sweep = await resolver.resweep(dag.id)
    second_sweep = await resolver.resweep(dag.id)  # b is already QUEUED now

    assert [t.id for t in first_sweep] == ["b"]
    assert second_sweep == []
    assert await queue.size() == 1  # not double-queued


async def test_is_complete_reflects_live_result_store_state(
    resolver: DAGResolver, results: ResultStore
) -> None:
    tasks = [Task(id=i, name="noop") for i in ("a", "b")]
    dag = DAG.from_tasks(tasks, [("a", "b")])
    await resolver.submit(dag)

    assert not await resolver.is_complete(dag.id)

    for task_id in ("a", "b"):
        t = await results.get(task_id)
        t.mark_running()
        t.mark_success(result=None)
        await results.save(t)

    assert await resolver.is_complete(dag.id)


async def test_list_task_ids_returns_none_for_unknown_dag(resolver: DAGResolver) -> None:
    assert await resolver.list_task_ids("does-not-exist") is None