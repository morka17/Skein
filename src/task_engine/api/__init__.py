"""
task_engine.api — the control-plane surface: FastAPI routes (tasks, DAGs,
health/metrics) and the WebSocket layer for real-time events.

Depends on task_engine.scheduler (Scheduler), task_engine.queue
(PriorityQueue, ResultStore, DAGResolver), and task_engine.monitoring
(EventPublisher, EventSubscriber). This is the outermost layer — nothing
else in the engine imports from here.
"""