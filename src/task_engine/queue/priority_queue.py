"""
The priority queue itself: a Redis sorted set (ZSET) scored by
(priority, submission time), plus a hash holding each queued task's full
serialized payload. Two structures instead of one because ZSET members
must be short/unique and we don't want to re-parse a huge JSON blob just
to compare scores — the ZSET only ever holds task IDs.

Uses BZPOPMIN for pop(): a blocking atomic "give me the single lowest-score
member" call, so workers aren't busy-polling an empty queue in a tight
loop — Redis itself blocks the connection until something arrives or the
timeout elapses.
"""

from __future__ import annotations

from typing import Optional

import redis.asyncio as redis

from task_engine.config import settings
from task_engine.core.task import Task
from task_engine.queue.redis_client import get_redis


def _score(priority: int, submitted_at_epoch_ms: float) -> float:
    """
    Lower score pops first. Priority dominates the ordering (multiplied up
    by a factor larger than any realistic epoch-ms timestamp component
    could flip), and submission time breaks ties FIFO within the same
    priority level. priority=0 (settings.min_priority) sorts lowest, i.e.
    most urgent — matches the "0 = urgent" convention in config.py.
    """
    return priority * 1e13 + submitted_at_epoch_ms


class PriorityQueue:
    def __init__(self, redis_client: Optional[redis.Redis] = None) -> None:
        self._redis = redis_client or get_redis()
        self._zset_key = f"{settings.key_prefix}:queue:zset"
        self._payload_key = f"{settings.key_prefix}:queue:payloads"

    async def push(self, task: Task) -> None:
        """
        Adds (or re-adds, on retry) a task to the queue. Does NOT mutate
        task.state — callers (worker.py, dag_resolver.py) are responsible
        for calling task.mark_queued() first, so the persisted payload
        here always reflects an accurate state.
        """
        score = _score(task.priority, task.updated_at.timestamp() * 1000)
        payload = task.model_dump_json()
        async with self._redis.pipeline(transaction=True) as pipe:
            pipe.hset(self._payload_key, task.id, payload)
            pipe.zadd(self._zset_key, {task.id: score})
            await pipe.execute()

    async def pop(self, timeout: float = 0.0) -> Optional[Task]:
        """
        Blocks up to `timeout` seconds waiting for the lowest-score
        (highest-priority, oldest) task. Returns None on timeout — callers
        (worker.py) treat that as "queue empty, loop and check shutdown".
        `timeout=0` blocks forever, which is why worker.py always passes
        `settings.worker_poll_interval` instead, to stay responsive to
        shutdown signals.
        """
        result = await self._redis.bzpopmin(self._zset_key, timeout=timeout)
        if result is None:
            return None

        _key, task_id, _score = result
        payload = await self._redis.hget(self._payload_key, task_id)
        if payload is None:
            # Popped from the ZSET but its payload is gone — shouldn't
            # happen under normal operation, but don't hand worker.py a
            # None task to execute; just report empty and let it re-poll.
            return None

        await self._redis.hdel(self._payload_key, task_id)
        return Task.model_validate_json(payload)

    async def remove(self, task_id: str) -> bool:
        """
        Cancels a task that's still sitting in the queue (not yet popped
        by a worker). Returns True if it was actually removed. Used by a
        future DELETE /tasks/{id} endpoint — has no effect on a task
        that's already RUNNING elsewhere.
        """
        async with self._redis.pipeline(transaction=True) as pipe:
            pipe.zrem(self._zset_key, task_id)
            pipe.hdel(self._payload_key, task_id)
            removed, _ = await pipe.execute()
        return bool(removed)

    async def size(self) -> int:
        """Current queue depth — surfaced by monitoring/metrics.py."""
        return await self._redis.zcard(self._zset_key)