"""
Lightweight observability: in-process counters are always available, with
an optional Prometheus exposition surface gated behind
settings.metrics_enabled and the `metrics` extra
(pip install -e ".[metrics]"). Importing this module is always safe even
without prometheus_client installed — only the Prometheus-specific
functions become real no-ops if it's missing.
"""

from __future__ import annotations

import logging
from collections import Counter
from typing import Awaitable, Callable

from task_engine.config import settings
from task_engine.core.states import TaskState
from task_engine.core.task import Task
from task_engine.queue.priority_queue import PriorityQueue

logger = logging.getLogger(__name__)


class InMemoryMetrics:
    """
    Process-local counters — sufficient for a single-process dev setup or
    for sanity-checking behavior in tests. In a multi-process deployment
    these counts are per-process, not global; that's fine under the
    Prometheus model below (each process's /metrics gets scraped
    independently and aggregated at query time), but don't read this
    object directly expecting a cluster-wide total.
    """

    def __init__(self) -> None:
        self.state_counts: Counter[str] = Counter()
        self.tasks_succeeded: int = 0
        self.tasks_failed: int = 0
        self.tasks_retried: int = 0

    def record(self, task: Task) -> None:
        self.state_counts[task.state.value] += 1
        if task.state == TaskState.SUCCESS:
            self.tasks_succeeded += 1
        elif task.state == TaskState.FAILED:
            self.tasks_failed += 1
        elif task.state == TaskState.RETRY:
            self.tasks_retried += 1

    def snapshot(self) -> dict[str, int]:
        return {
            "tasks_succeeded": self.tasks_succeeded,
            "tasks_failed": self.tasks_failed,
            "tasks_retried": self.tasks_retried,
            **{f"state.{k}": v for k, v in self.state_counts.items()},
        }


# Process-wide instance. `record_event` below is EventHook-shaped, so this
# can be wired in directly — e.g. Scheduler(downstream_event=record_event)
# — as a zero-dependency alternative to the Prometheus path.
default_metrics = InMemoryMetrics()


async def record_event(task: Task) -> None:
    default_metrics.record(task)


async def queue_depth(queue: PriorityQueue) -> int:
    """Current backlog size — the number that actually matters for
    alerting ('is the queue draining or growing'), more so than raw
    cumulative throughput."""
    return await queue.size()


# ------------------------------------------------------------------
# Optional Prometheus exposition
# ------------------------------------------------------------------

MetricsEventHook = Callable[[Task], Awaitable[None]]

_prometheus_available = False
if settings.metrics_enabled:
    try:
        from prometheus_client import Counter as PromCounter
        from prometheus_client import Gauge, generate_latest

        _prometheus_available = True
    except ImportError:
        logger.warning(
            "settings.metrics_enabled is True but prometheus_client isn't "
            "installed — install the `metrics` extra: pip install -e \".[metrics]\""
        )

if _prometheus_available:
    TASKS_TOTAL = PromCounter(
        "task_engine_tasks_total", "Tasks observed, by state", ["state"]
    )
    QUEUE_DEPTH = Gauge("task_engine_queue_depth", "Current priority queue backlog size")

    async def record_prometheus(task: Task) -> None:
        """EventHook-compatible — wire as Scheduler(downstream_event=record_prometheus)."""
        TASKS_TOTAL.labels(state=task.state.value).inc()

    async def update_queue_depth_gauge(queue: PriorityQueue) -> None:
        """Call periodically (e.g. from scheduler.py's reconciliation tick,
        or a dedicated background task) — queue depth isn't event-driven,
        it has to be sampled."""
        QUEUE_DEPTH.set(await queue.size())

    def metrics_endpoint() -> bytes:
        """Raw exposition text for a `GET /metrics` route — return with
        Content-Type: text/plain; version=0.0.4; charset=utf-8."""
        return generate_latest()

else:

    async def record_prometheus(task: Task) -> None:  # type: ignore[misc]
        return None

    async def update_queue_depth_gauge(queue: PriorityQueue) -> None:  # type: ignore[misc]
        return None

    def metrics_endpoint() -> bytes:  # type: ignore[misc]
        return (
            b"# metrics disabled: set TASK_ENGINE_METRICS_ENABLED=true and "
            b"install the `metrics` extra\n"
        )