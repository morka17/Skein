"""
The full path, end to end: submit a fan-out/fan-in DAG via
DAGResolver.submit(), run a real WorkerPool with Scheduler.on_event wired
in, and confirm the DAG reaches completion with every node in SUCCESS —
exercising queue/, worker/, and scheduler/ together exactly as
api/main.py and scripts/run_worker.py wire them in production, just
against fakeredis instead of a real Redis server.
"""

from __future__ import annotations

import asyncio

import pytest

from task_engine.core.dag import DAG
from task_engine.core.states import TaskState
from task_engine.core.task import Task
from task_engine.queue.dag_resolver import DAGResolver
from task_engine.queue.priority_queue import PriorityQueue
from task_engine.queue.result_store import ResultStore
from task_engine.scheduler.scheduler import Scheduler
from task_engine.worker.pool import WorkerPool
from task_engine.worker.registry import TaskRegistry


@pytest.fixture
def registry() -> TaskRegistry:
    reg = TaskRegistry()

    async def fetch() -> dict:
        await asyncio.sleep(0.01)
        return {"rows": [1, 2, 3]}

    async def transform(field: str) -> dict:
        await asyncio.sleep(0.01)
        return {"field": field, "transformed": True}

    async def load() -> str:
        await asyncio.sleep(0.01)
        return "loaded"

    reg.register("fetch", fetch)
    reg.register("transform", transform)
    reg.register("load", load)
    return reg


def _build_fan_out_fan_in_dag() -> DAG:
    """fetch -> {transform_a, transform_b} -> load — same shape as
    examples/dag_pipeline.py, kept small and self-contained here so this
    test doesn't depend on the examples package."""
    tasks = [
        Task(id="fetch", name="fetch"),
        Task(id="transform_a", name="transform", payload={"field": "a"}),
        Task(id="transform_b", name="transform", payload={"field": "b"}),
        Task(id="load", name="load"),
    ]
    edges = [
        ("fetch", "transform_a"),
        ("fetch", "transform_b"),
        ("transform_a", "load"),
        ("transform_b", "load"),
    ]
    return DAG.from_tasks(tasks, edges)


async def test_full_dag_runs_to_completion_through_a_real_worker_pool(
    queue: PriorityQueue,
    results: ResultStore,
    resolver: DAGResolver,
    redis_client,
    registry: TaskRegistry,
) -> None:
    dag = _build_fan_out_fan_in_dag()

    scheduler = Scheduler(
        queue=queue, results=results, resolver=resolver, redis_client=redis_client
    )
    await scheduler.submit_dag(dag)

    pool = WorkerPool(
        queue=queue,
        results=results,
        registry=registry,
        concurrency=2,
        on_event=scheduler.on_event,  # the line that actually wires DAG-unlocking in
    )

    pool_task = asyncio.create_task(pool.start())
    try:
        # Poll for completion rather than sleeping a fixed duration — as
        # fast as the DAG actually finishes, with a generous ceiling so a
        # genuine deadlock fails the test instead of hanging forever.
        for _ in range(300):
            if await resolver.is_complete(dag.id):
                break
            await asyncio.sleep(0.01)
        else:
            pytest.fail("DAG did not reach completion within the test window")
    finally:
        pool.shutdown()
        await pool.wait_closed()
        pool_task.cancel()

    final_states = await results.get_many(["fetch", "transform_a", "transform_b", "load"])
    assert len(final_states) == 4
    for task_id, task in final_states.items():
        assert task.state == TaskState.SUCCESS, f"{task_id} ended in {task.state}, not SUCCESS"

    # transform_a and transform_b must both have fetch as their recorded
    # dependency — confirms DAG.from_tasks() stamped depends_on correctly,
    # not just that execution happened to work out.
    assert final_states["transform_a"].depends_on == ["fetch"]
    assert final_states["transform_b"].depends_on == ["fetch"]
    assert set(final_states["load"].depends_on) == {"transform_a", "transform_b"}

    assert await resolver.is_complete(dag.id)