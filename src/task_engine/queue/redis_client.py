"""
Single shared async Redis connection pool for the whole process. Every
other module (PriorityQueue, ResultStore, DAGResolver, and later
monitoring/pubsub.py) gets its connection via get_redis() rather than
constructing its own — one pool per process keeps the connection count
bounded regardless of how many of those objects get instantiated.
"""

from __future__ import annotations

from typing import Optional

import redis.asyncio as redis

from task_engine.config import settings

_redis: Optional[redis.Redis] = None


def get_redis() -> redis.Redis:
    """
    Returns the process-wide Redis client, creating it (and its connection
    pool) on first call. `decode_responses=True` means every module gets
    plain `str` back from Redis, not `bytes` — one less thing for
    priority_queue.py / result_store.py to handle.
    """
    global _redis
    if _redis is None:
        pool = redis.ConnectionPool.from_url(
            settings.redis_url,
            max_connections=settings.redis_max_connections,
            socket_timeout=settings.redis_socket_timeout,
            decode_responses=True,
        )
        _redis = redis.Redis(connection_pool=pool)
    return _redis


async def close_redis() -> None:
    """Call during application shutdown (scripts/run_worker.py, api/main.py)
    to release the connection pool cleanly instead of leaking sockets."""
    global _redis
    if _redis is not None:
        await _redis.aclose()
        _redis = None


async def ping() -> bool:
    """Health check used by api/routes/health.py — never raises, just
    reports reachability."""
    try:
        return bool(await get_redis().ping())
    except Exception:  # noqa: BLE001 — a health check must never itself crash
        return False