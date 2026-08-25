"""
Persists the full state of every Task — not just terminal results. Every
call to a Task.mark_*() method in worker.py or dag_resolver.py is followed
by a save() here, so `GET /tasks/{id}` (api/routes/tasks.py) and the
DAG resolver's dependency checks both read from the same source of truth,
regardless of which worker process last touched the task.
"""

from __future__ import annotations

from typing import Iterable, Optional

import redis.asyncio as redis

from task_engine.config import settings
from task_engine.core.task import Task
from task_engine.queue.redis_client import get_redis


class ResultStore:
    def __init__(self, redis_client: Optional[redis.Redis] = None) -> None:
        self._redis = redis_client or get_redis()
        self._prefix = f"{settings.key_prefix}:task:"

    def _key(self, task_id: str) -> str:
        return f"{self._prefix}{task_id}"

    async def save(self, task: Task) -> None:
        """
        Overwrites the stored record for this task. Terminal states
        (SUCCESS/FAILED/CANCELLED — see Task.is_terminal) get an expiry
        set so completed tasks don't accumulate in Redis forever;
        in-flight tasks are never expired.
        """
        key = self._key(task.id)
        await self._redis.set(key, task.model_dump_json())
        if task.is_terminal:
            await self._redis.expire(key, settings.result_ttl_seconds)

    async def get(self, task_id: str) -> Optional[Task]:
        payload = await self._redis.get(self._key(task_id))
        if payload is None:
            return None
        return Task.model_validate_json(payload)

    async def get_many(self, task_ids: Iterable[str]) -> dict[str, Task]:
        """
        Batched read via MGET — used by dag_resolver.py, which needs the
        current state of every node in a DAG at once rather than N round
        trips. Missing keys (expired or never written) are simply omitted
        from the result rather than raising.
        """
        ids = list(task_ids)
        if not ids:
            return {}
        values = await self._redis.mget([self._key(tid) for tid in ids])
        return {
            tid: Task.model_validate_json(payload)
            for tid, payload in zip(ids, values)
            if payload is not None
        }

    async def delete(self, task_id: str) -> None:
        await self._redis.delete(self._key(task_id))