"""
The I/O bridge between core.DAG's pure graph algorithms and live Redis
state. Owns two jobs:

  1. submit(dag) — persist every node's initial state, push root nodes
     (no parents) onto the priority queue, leave everything else PENDING.
  2. on_task_success(task) — given a task that just reached SUCCESS, ask
     the DAG which children are now unlocked and push those.

The DAG's structure (which task IDs belong to it, and the edge list) is
stored in Redis under its own key — separately from individual task state
in ResultStore — so a resolver running in a different process, or after a
restart, can reconstruct "what should happen next" without ever holding
the DAG object in memory.
"""

from __future__ import annotations

import json
import logging
from typing import Optional

import redis.asyncio as redis

from task_engine.config import settings
from task_engine.core.dag import DAG
from task_engine.core.states import TaskState
from task_engine.core.task import Task
from task_engine.queue.priority_queue import PriorityQueue
from task_engine.queue.redis_client import get_redis
from task_engine.queue.result_store import ResultStore

logger = logging.getLogger(__name__)


class DAGResolver:
    def __init__(
        self,
        queue: PriorityQueue,
        results: ResultStore,
        redis_client: Optional[redis.Redis] = None,
    ) -> None:
        self._redis = redis_client or get_redis()
        self._queue = queue
        self._results = results
        self._dag_key_prefix = f"{settings.key_prefix}:dag:"

    def _dag_key(self, dag_id: str) -> str:
        return f"{self._dag_key_prefix}{dag_id}"

    # ------------------------------------------------------------------
    # Submission
    # ------------------------------------------------------------------

    async def submit(self, dag: DAG) -> list[Task]:
        """
        Persists the DAG's structure and every node's initial (PENDING)
        state, then queues the root nodes (no parents) so execution
        actually starts. `dag` must already be validated — call
        DAG.from_tasks(...) to build it; this method assumes it's acyclic.

        Returns the root tasks that were queued, for the caller (typically
        api/routes/dags.py) to report back or publish events for.
        """
        meta = json.dumps(
            {"task_ids": list(dag.tasks.keys()), "edges": [list(e) for e in dag.edges]}
        )
        await self._redis.set(self._dag_key(dag.id), meta)

        for task in dag.tasks.values():
            await self._results.save(task)  # PENDING state, visible via GET /tasks/{id}

        queued: list[Task] = []
        for root_id in dag.root_nodes():
            task = dag.tasks[root_id]
            task.mark_queued()
            await self._results.save(task)
            await self._queue.push(task)
            queued.append(task)

        logger.info(
            "dag %s submitted: %d nodes, %d queued immediately",
            dag.id, len(dag.tasks), len(queued),
        )
        return queued

    # ------------------------------------------------------------------
    # Progression — called by scheduler/dependency_tracker.py whenever a
    # task belonging to a DAG reaches SUCCESS
    # ------------------------------------------------------------------

    async def on_task_success(self, task: Task) -> list[Task]:
        """
        Determines which sibling/child nodes are now unlocked by `task`
        succeeding, marks them QUEUED, pushes them to the priority queue,
        and returns them. A no-op (returns []) for standalone tasks
        (dag_id is None). Delegates to resweep() — see its docstring for
        why re-deriving from scratch, rather than diffing off `task`
        specifically, is the safer approach.
        """
        if task.dag_id is None:
            return []
        return await self.resweep(task.dag_id)

    async def resweep(self, dag_id: str) -> list[Task]:
        """
        Re-derives every currently-ready node in a DAG directly from
        ResultStore state, independent of which specific task triggered
        the check. Used both by on_task_success() (the reactive path) and
        by scheduler.py's periodic reconciliation loop (the safety-net
        path) — safe to call redundantly, since it only queues nodes still
        in PENDING; anything already QUEUED/RUNNING/terminal is left alone.
        """
        meta = await self._load_meta(dag_id)
        if meta is None:
            logger.warning("dag %s metadata missing — cannot resolve dependents", dag_id)
            return []
        task_ids, edges = meta

        all_tasks = await self._results.get_many(task_ids)
        succeeded_ids = {tid for tid, t in all_tasks.items() if t.state == TaskState.SUCCESS}

        # Rebuild a DAG view purely for its graph queries — already known
        # to be acyclic (validated at submit time), so this skips
        # re-running cycle detection.
        dag = DAG(id=dag_id, tasks=all_tasks, edges=edges)
        ready_ids = dag.ready_nodes(succeeded_ids)

        newly_queued: list[Task] = []
        for node_id in ready_ids:
            node = all_tasks[node_id]
            if node.state != TaskState.PENDING:
                # Already QUEUED/RUNNING/terminal — guards against two
                # parents finishing near-simultaneously both trying to
                # unlock the same child, and against a redundant call from
                # scheduler.py's reconciliation loop.
                continue
            node.mark_queued()
            await self._results.save(node)
            await self._queue.push(node)
            newly_queued.append(node)

        return newly_queued

    async def list_task_ids(self, dag_id: str) -> Optional[list[str]]:
        """All task ids belonging to a DAG, or None if the DAG doesn't
        exist. Used by api/routes/dags.py to assemble a full status
        response without duplicating _load_meta's parsing logic."""
        meta = await self._load_meta(dag_id)
        if meta is None:
            return None
        task_ids, _edges = meta
        return task_ids

    async def is_complete(self, dag_id: str) -> bool:
        """True once every node in the DAG has reached SUCCESS. Used to
        fire DagCompleted (monitoring/events.py)."""
        meta = await self._load_meta(dag_id)
        if meta is None:
            return False
        task_ids, edges = meta
        all_tasks = await self._results.get_many(task_ids)
        succeeded_ids = {tid for tid, t in all_tasks.items() if t.state == TaskState.SUCCESS}
        dag = DAG(id=dag_id, tasks=all_tasks, edges=edges)
        return dag.is_complete(succeeded_ids)

    # ------------------------------------------------------------------

    async def _load_meta(self, dag_id: str) -> Optional[tuple[list[str], list[tuple[str, str]]]]:
        raw = await self._redis.get(self._dag_key(dag_id))
        if raw is None:
            return None
        data = json.loads(raw)
        edges = [tuple(e) for e in data["edges"]]
        return data["task_ids"], edges