"""
Top-level orchestration surface — the object api/routes/ and
scripts/run_worker.py actually talk to. Wires PriorityQueue, ResultStore,
DAGResolver, and DependencyTracker together and exposes three things:

  - submit_task() / submit_dag() — entry points for new work
  - on_event — the EventHook to hand to WorkerPool, driving the reactive
    DAG-unlocking path (see dependency_tracker.py)
  - run() — a background reconciliation loop: a safety net that
    periodically re-derives DAG readiness in case a state-change event
    was ever dropped (e.g. a worker process crashed between
    task.mark_success() and its on_event() call actually firing)
"""

from __future__ import annotations

import asyncio
import logging
from typing import Optional

import redis.asyncio as redis

from task_engine.config import settings
from task_engine.core.dag import DAG
from task_engine.core.task import Task
from task_engine.queue.dag_resolver import DAGResolver
from task_engine.queue.priority_queue import PriorityQueue
from task_engine.queue.redis_client import get_redis
from task_engine.queue.result_store import ResultStore
from task_engine.scheduler.dependency_tracker import DependencyTracker, EventHook

logger = logging.getLogger(__name__)


class Scheduler:
    def __init__(
        self,
        queue: Optional[PriorityQueue] = None,
        results: Optional[ResultStore] = None,
        resolver: Optional[DAGResolver] = None,
        redis_client: Optional[redis.Redis] = None,
        downstream_event: Optional[EventHook] = None,
    ) -> None:
        self._redis = redis_client or get_redis()
        self.queue = queue or PriorityQueue(self._redis)
        self.results = results or ResultStore(self._redis)
        self.resolver = resolver or DAGResolver(self.queue, self.results, self._redis)
        self.tracker = DependencyTracker(
            self.resolver,
            on_dag_complete=self._on_dag_complete,
            downstream=downstream_event,
        )

        self._active_dags_key = f"{settings.key_prefix}:scheduler:active_dags"
        self._stop_event = asyncio.Event()

    @property
    def on_event(self) -> EventHook:
        """Pass to WorkerPool(on_event=scheduler.on_event) — this is what
        actually drives DAG children getting unlocked as their parents
        complete, and what forwards every state change to `downstream_event`
        (e.g. monitoring/pubsub.py, once wired up) for real-time reporting."""
        return self.tracker.handle_event

    # ------------------------------------------------------------------
    # Entry points — called by api/routes/tasks.py and api/routes/dags.py
    # ------------------------------------------------------------------

    async def submit_task(self, task: Task) -> Task:
        """Queues a standalone task (no DAG) for immediate execution."""
        task.mark_queued()
        await self.results.save(task)
        await self.queue.push(task)
        return task

    async def submit_dag(self, dag: DAG) -> list[Task]:
        """Registers a DAG and queues its root nodes. `dag` should already
        be constructed via DAG.from_tasks(...) — structural validation
        (cycles, depth) happens there, not here."""
        queued = await self.resolver.submit(dag)
        await self._redis.sadd(self._active_dags_key, dag.id)
        return queued

    # ------------------------------------------------------------------
    # Reconciliation loop
    # ------------------------------------------------------------------

    async def run(self) -> None:
        """
        Runs until stop() is called (or the process exits). Safe to run in
        the same process as a WorkerPool, or as a separate deployment —
        it only ever re-derives state from ResultStore, never guesses.
        Typically started as a background task in scripts/run_api.py or
        its own scripts/run_scheduler.py.
        """
        logger.info(
            "scheduler reconciliation loop started (tick=%.1fs)",
            settings.scheduler_tick_seconds,
        )
        while not self._stop_event.is_set():
            try:
                await self._reconcile_once()
            except Exception:
                # A bad tick must never kill the loop — log and try again
                # next interval rather than leaving every active DAG
                # without its safety net.
                logger.exception("scheduler reconciliation tick failed")
            await asyncio.sleep(settings.scheduler_tick_seconds)
        logger.info("scheduler reconciliation loop stopped")

    def stop(self) -> None:
        self._stop_event.set()

    async def _reconcile_once(self) -> None:
        dag_ids = await self._redis.smembers(self._active_dags_key)
        for dag_id in dag_ids:
            if await self.resolver.is_complete(dag_id):
                await self._on_dag_complete(dag_id)
                continue

            unlocked = await self.resolver.resweep(dag_id)
            if unlocked:
                # This firing at all means the reactive path (worker.py's
                # on_event -> DependencyTracker) missed something — worth
                # a WARNING, since it's a signal something upstream broke.
                logger.warning(
                    "reconciliation unlocked %d task(s) in dag %s that the "
                    "reactive path missed: %s",
                    len(unlocked), dag_id, [t.id for t in unlocked],
                )

    async def _on_dag_complete(self, dag_id: str) -> None:
        await self._redis.srem(self._active_dags_key, dag_id)
        logger.info("dag %s complete", dag_id)