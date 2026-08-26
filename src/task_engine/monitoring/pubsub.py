"""
Redis pub/sub transport for real-time events. Two channel scopes:
  - a per-DAG channel, so a client watching one DAG execute doesn't get
    flooded with traffic from every other DAG running concurrently
  - one global channel, for a system-wide "all activity" dashboard

`EventPublisher.publish_task_event` is deliberately EventHook-shaped —
pass it straight to `Scheduler(downstream_event=publisher.publish_task_event)`
so every task state change flows into pub/sub automatically, without
worker.py or scheduler.py importing this module or knowing pub/sub exists.
"""

from __future__ import annotations

import logging
from typing import AsyncIterator, Optional

import redis.asyncio as redis

from task_engine.config import settings
from task_engine.core.task import Task
from task_engine.monitoring.events import Event, dag_completed_event, event_from_task
from task_engine.queue.redis_client import get_redis

logger = logging.getLogger(__name__)

_GLOBAL_CHANNEL = f"{settings.key_prefix}:events:global"


def _dag_channel(dag_id: str) -> str:
    return f"{settings.key_prefix}:events:dag:{dag_id}"


class EventPublisher:
    def __init__(self, redis_client: Optional[redis.Redis] = None) -> None:
        self._redis = redis_client or get_redis()

    async def publish(self, event: Event) -> None:
        """Publishes to the global channel always, plus the event's DAG
        channel if it has one — a standalone task's events only ever hit
        the global channel."""
        payload = event.model_dump_json()
        channels = [_GLOBAL_CHANNEL]
        dag_id = getattr(event, "dag_id", None)
        if dag_id:
            channels.append(_dag_channel(dag_id))

        for channel in channels:
            await self._redis.publish(channel, payload)

    async def publish_task_event(self, task: Task) -> None:
        """
        EventHook-compatible entry point — wire this as
        Scheduler(downstream_event=...) and every worker/scheduler state
        change reaches here automatically. Silently no-ops for states with
        no corresponding event (PENDING): a hook in this chain must never
        raise, since worker.py awaits it inline after every task outcome.
        """
        try:
            event = event_from_task(task)
        except ValueError:
            return
        await self.publish(event)

    async def publish_dag_completed(self, dag_id: str) -> None:
        """
        DagCompleteHook-compatible — wire this as
        Scheduler(on_dag_complete=publisher.publish_dag_completed).
        """
        await self.publish(dag_completed_event(dag_id))


class EventSubscriber:
    """
    Usage (api/websocket.py):

        subscriber = EventSubscriber()
        async for raw_json in subscriber.stream(dag_id=dag_id):
            await websocket.send_text(raw_json)
    """

    def __init__(self, redis_client: Optional[redis.Redis] = None) -> None:
        self._redis = redis_client or get_redis()

    async def stream(self, dag_id: Optional[str] = None) -> AsyncIterator[str]:
        """
        Yields raw JSON strings as they arrive. Subscribes to a single
        DAG's channel if `dag_id` is given, else the global firehose —
        one call to this per WebSocket connection, iterated for the life
        of that connection.
        """
        channel = _dag_channel(dag_id) if dag_id else _GLOBAL_CHANNEL
        pubsub = self._redis.pubsub()
        await pubsub.subscribe(channel)
        logger.info("subscribed to %s", channel)
        try:
            async for message in pubsub.listen():
                if message["type"] != "message":
                    continue  # skip the subscribe-confirmation message itself
                yield message["data"]
        finally:
            await pubsub.unsubscribe(channel)
            await pubsub.aclose()