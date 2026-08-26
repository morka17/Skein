"""
Shared fixtures. Every test in this suite runs against fakeredis — an
in-memory, drop-in-compatible Redis implementation — so `pip install
-e ".[dev]"` is enough to run the whole suite; no Redis server required.

Each test gets its own fresh fakeredis instance (function-scoped), so
tests never see each other's keys and can run in any order, including
in parallel with pytest-xdist if you add it later.
"""

from __future__ import annotations

from typing import AsyncIterator

import pytest_asyncio
from fakeredis.aioredis import FakeRedis

from task_engine.queue.dag_resolver import DAGResolver
from task_engine.queue.priority_queue import PriorityQueue
from task_engine.queue.result_store import ResultStore


@pytest_asyncio.fixture
async def redis_client() -> AsyncIterator[FakeRedis]:
    client = FakeRedis(decode_responses=True)
    try:
        yield client
    finally:
        await client.aclose()


@pytest_asyncio.fixture
async def queue(redis_client: FakeRedis) -> PriorityQueue:
    return PriorityQueue(redis_client)


@pytest_asyncio.fixture
async def results(redis_client: FakeRedis) -> ResultStore:
    return ResultStore(redis_client)


@pytest_asyncio.fixture
async def resolver(
    queue: PriorityQueue, results: ResultStore, redis_client: FakeRedis
) -> DAGResolver:
    return DAGResolver(queue, results, redis_client)