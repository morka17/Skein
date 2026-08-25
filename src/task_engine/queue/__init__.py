"""
task_engine.queue — Redis mechanics: connection pool, priority queue,
result/state persistence, and DAG dependency resolution.

Depends on task_engine.core (Task, DAG) only; nothing here imports from
worker/, scheduler/, or api/ — those layers depend on this one, never the
reverse.
"""

from task_engine.queue.dag_resolver import DAGResolver
from task_engine.queue.priority_queue import PriorityQueue
from task_engine.queue.redis_client import close_redis, get_redis, ping
from task_engine.queue.result_store import ResultStore

__all__ = [
    "PriorityQueue",
    "ResultStore",
    "DAGResolver",
    "get_redis",
    "close_redis",
    "ping",
]