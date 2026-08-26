"""
task_engine.monitoring — real-time status: event schema, Redis pub/sub
transport, and metrics (in-process + optional Prometheus).

Depends on task_engine.core (Task) and task_engine.queue (PriorityQueue,
for depth sampling) only. Nothing in worker/ or scheduler/ imports this
package directly — they expose EventHook-shaped callbacks
(`on_event` / `downstream_event` / `on_dag_complete`) that this package's
functions are designed to plug into, e.g.:

    publisher = EventPublisher()
    scheduler = Scheduler(
        downstream_event=publisher.publish_task_event,
        on_dag_complete=publisher.publish_dag_completed,
    )
    pool = WorkerPool(queue, results, on_event=scheduler.on_event)
"""

from task_engine.monitoring.events import (
    DagCompletedEvent,
    Event,
    TaskEvent,
    dag_completed_event,
    event_from_task,
)
from task_engine.monitoring.metrics import (
    InMemoryMetrics,
    default_metrics,
    metrics_endpoint,
    queue_depth,
    record_event,
    record_prometheus,
    update_queue_depth_gauge,
)
from task_engine.monitoring.pubsub import EventPublisher, EventSubscriber

__all__ = [
    "TaskEvent",
    "DagCompletedEvent",
    "Event",
    "event_from_task",
    "dag_completed_event",
    "EventPublisher",
    "EventSubscriber",
    "InMemoryMetrics",
    "default_metrics",
    "record_event",
    "record_prometheus",
    "queue_depth",
    "update_queue_depth_gauge",
    "metrics_endpoint",
]