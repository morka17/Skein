"""
Worker process entrypoint:  python scripts/run_worker.py

Imports whatever module(s) register your @task functions, then starts a
WorkerPool that pulls from the shared Redis-backed queue. Run as many of
these as you like, on as many hosts as you like, all pointed at the same
Redis — that's the "distributed" in "distributed task execution engine".

This process also builds a Scheduler purely for its `on_event` hook: every
time a task here finishes, on_event drives DependencyTracker (unlocking
DAG children via DAGResolver) and publishes to Redis pub/sub for
api/websocket.py's live subscribers. The API process (run_api.py) owns
submission and the periodic reconciliation safety-net loop; this process
owns execution and the reactive DAG-unlocking path — the two coordinate
entirely through Redis, never by talking to each other directly.
"""

from __future__ import annotations

import asyncio
import logging
import sys
from pathlib import Path

# Puts the repo root on sys.path so `import examples...` resolves when this
# is run directly (`python scripts/run_worker.py`) rather than via an
# installed console script. Harmless no-op if the repo root is already on
# the path (e.g. running under `pip install -e .`).
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from task_engine.config import settings
from task_engine.monitoring.pubsub import EventPublisher
from task_engine.queue.dag_resolver import DAGResolver
from task_engine.queue.priority_queue import PriorityQueue
from task_engine.queue.redis_client import close_redis, get_redis
from task_engine.queue.result_store import ResultStore
from task_engine.scheduler.scheduler import Scheduler
from task_engine.worker.pool import WorkerPool

# --------------------------------------------------------------------
# Register your @task functions by importing the module(s) that define
# them, BEFORE the pool starts — replace these with your own:
#
#   import myapp.tasks  # noqa: F401
#
# Left in place, these two give you a runnable demo out of the box:
# --------------------------------------------------------------------
import examples.simple_task  # noqa: F401,E402
import examples.dag_pipeline  # noqa: F401,E402

logging.basicConfig(level=settings.log_level)
logger = logging.getLogger(__name__)


async def main() -> None:
    redis_client = get_redis()
    queue = PriorityQueue(redis_client)
    results = ResultStore(redis_client)
    resolver = DAGResolver(queue, results, redis_client)
    publisher = EventPublisher(redis_client)

    scheduler = Scheduler(
        queue=queue,
        results=results,
        resolver=resolver,
        redis_client=redis_client,
        downstream_event=publisher.publish_task_event,
        on_dag_complete=publisher.publish_dag_completed,
    )

    pool = WorkerPool(queue=queue, results=results, on_event=scheduler.on_event)

    logger.info("worker process starting — registered tasks: %s", pool.registry.names())
    try:
        await pool.start()  # blocks until SIGINT/SIGTERM, then drains in-flight tasks
    finally:
        await close_redis()


def run() -> None:
    asyncio.run(main())


if __name__ == "__main__":
    run()