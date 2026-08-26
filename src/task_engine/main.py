"""
FastAPI application entrypoint. Wires every layer together for a single
process: Redis-backed queue + result store, the DAG resolver, the
scheduler (reactive DAG unlocking, via its on_event hook, plus the
reconciliation loop as a safety net), and the monitoring publisher — then
exposes all of it over HTTP and WebSocket.

Important: this process does NOT execute tasks itself. Run
scripts/run_worker.py — one or many, on this host or others — alongside
this to actually drain the queue. main.py is the control plane; workers
are the execution plane. This separation is what lets the two scale
independently.
"""

from __future__ import annotations

import asyncio
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from task_engine.api.routes import dags, health, tasks
from task_engine.api.websocket import router as websocket_router
from task_engine.config import settings
from task_engine.monitoring.pubsub import EventPublisher
from task_engine.queue.dag_resolver import DAGResolver
from task_engine.queue.priority_queue import PriorityQueue
from task_engine.queue.redis_client import close_redis, get_redis
from task_engine.queue.result_store import ResultStore
from task_engine.scheduler.scheduler import Scheduler

logging.basicConfig(level=settings.log_level)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    redis_client = get_redis()
    queue = PriorityQueue(redis_client)
    results = ResultStore(redis_client)
    resolver = DAGResolver(queue, results, redis_client)
    publisher = EventPublisher(redis_client)

    # This is the one place every layer built so far actually gets
    # connected: worker.py's state changes -> Scheduler.on_event ->
    # DependencyTracker (unlocks DAG children) -> downstream_event ->
    # EventPublisher (Redis pub/sub) -> api/websocket.py's subscribers.
    scheduler = Scheduler(
        queue=queue,
        results=results,
        resolver=resolver,
        redis_client=redis_client,
        downstream_event=publisher.publish_task_event,
        on_dag_complete=publisher.publish_dag_completed,
    )

    app.state.queue = queue
    app.state.results = results
    app.state.resolver = resolver
    app.state.scheduler = scheduler
    app.state.publisher = publisher

    reconciliation_task = asyncio.create_task(scheduler.run())
    logger.info("task_engine API started")

    yield

    scheduler.stop()
    reconciliation_task.cancel()
    try:
        await reconciliation_task
    except asyncio.CancelledError:
        pass
    await close_redis()
    logger.info("task_engine API shut down")


app = FastAPI(
    title="Skein",
    description=(
        "A distributed task execution engine — priority scheduling, "
        "DAG-aware sub-task dependencies, and real-time execution monitoring."
    ),
    version="0.1.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(health.router)
app.include_router(tasks.router)
app.include_router(dags.router)
app.include_router(websocket_router)