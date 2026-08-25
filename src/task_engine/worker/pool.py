"""
Spawns and supervises N concurrent `run_worker` coroutines within a single
process. This is the "concurrency" in "distributed task execution engine":
one WorkerPool == one process/container running `settings.worker_concurrency`
coroutines; horizontal scale comes from running multiple pool instances
(one per host/container), all pulling from the same Redis-backed queue.
"""

from __future__ import annotations

import asyncio
import logging
import signal
import uuid
from typing import Optional

from task_engine.config import settings
from task_engine.queue.priority_queue import PriorityQueue
from task_engine.queue.result_store import ResultStore
from task_engine.worker.registry import TaskRegistry, default_registry
from task_engine.worker.retry import RetryPolicy
from task_engine.worker.worker import EventHook, _default_hook, run_worker

logger = logging.getLogger(__name__)


class WorkerPool:
    """
    Usage (see scripts/run_worker.py):

        pool = WorkerPool(queue=priority_queue, results=result_store)
        await pool.start()   # blocks until SIGINT/SIGTERM, then drains gracefully
    """

    def __init__(
        self,
        queue: PriorityQueue,
        results: ResultStore,
        registry: TaskRegistry = default_registry,
        concurrency: Optional[int] = None,
        retry_policy: Optional[RetryPolicy] = None,
        on_event: EventHook = _default_hook,
    ) -> None:
        self.queue = queue
        self.results = results
        self.registry = registry
        self.concurrency = concurrency or settings.worker_concurrency
        self.retry_policy = retry_policy or RetryPolicy()
        self.on_event = on_event

        self.pool_id = uuid.uuid4().hex[:8]
        self._shutdown_event = asyncio.Event()
        self._tasks: list[asyncio.Task] = []

    async def start(self) -> None:
        """Spawns `self.concurrency` worker coroutines and blocks until
        they all exit (i.e. until shutdown() is called and in-flight
        tasks finish draining)."""
        logger.info(
            "starting worker pool %s: concurrency=%d, registered_tasks=%s",
            self.pool_id, self.concurrency, self.registry.names(),
        )

        self._install_signal_handlers()

        self._tasks = [
            asyncio.create_task(
                run_worker(
                    worker_id=f"{self.pool_id}-{i}",
                    queue=self.queue,
                    results=self.results,
                    registry=self.registry,
                    retry_policy=self.retry_policy,
                    on_event=self.on_event,
                    shutdown_event=self._shutdown_event,
                ),
                name=f"worker-{self.pool_id}-{i}",
            )
            for i in range(self.concurrency)
        ]

        await asyncio.gather(*self._tasks)
        logger.info("worker pool %s stopped", self.pool_id)

    def _install_signal_handlers(self) -> None:
        loop = asyncio.get_event_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, self.shutdown)
            except NotImplementedError:
                # Windows event loops don't support add_signal_handler —
                # shutdown() can still be called manually there.
                pass

    def shutdown(self) -> None:
        """Signals all worker loops to stop polling for NEW tasks. Tasks
        already RUNNING are allowed to finish — this does not cancel them,
        so a slow task in flight won't be killed mid-execution."""
        if not self._shutdown_event.is_set():
            logger.info("worker pool %s: shutdown requested, draining in-flight tasks", self.pool_id)
            self._shutdown_event.set()

    async def wait_closed(self) -> None:
        """Await this after calling shutdown() from elsewhere (e.g. an API
        endpoint) to know when every worker coroutine has actually exited."""
        await asyncio.gather(*self._tasks, return_exceptions=True)