"""
    WS /ws/dags/{dag_id}   events for one DAG's execution only
    WS /ws/events          every event in the system (dashboard firehose)

This is the "real-time status monitoring" requirement made visible over
the wire — a thin adapter over monitoring/pubsub.py's Redis subscriber,
which does the actual work of listening for events.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Optional

from fastapi import APIRouter, WebSocket, WebSocketDisconnect

from task_engine.config import settings
from task_engine.monitoring.pubsub import EventSubscriber

logger = logging.getLogger(__name__)
router = APIRouter(tags=["websocket"])


async def _heartbeat(websocket: WebSocket) -> None:
    """
    Keeps the connection alive through proxies/load balancers that close
    idle WebSocket connections — sends a ping if no real event fires for
    a while. Runs as a separate task alongside the event stream below;
    exits quietly once the socket is gone.
    """
    try:
        while True:
            await asyncio.sleep(settings.websocket_heartbeat_seconds)
            await websocket.send_json({"type": "ping"})
    except Exception:  # noqa: BLE001 — socket closed/errored; nothing to do but stop pinging
        return


async def _stream(websocket: WebSocket, dag_id: Optional[str]) -> None:
    await websocket.accept()
    subscriber = EventSubscriber()
    heartbeat_task = asyncio.create_task(_heartbeat(websocket))

    try:
        async for raw_json in subscriber.stream(dag_id=dag_id):
            await websocket.send_text(raw_json)
    except WebSocketDisconnect:
        logger.info("websocket disconnected (dag_id=%s)", dag_id)
    finally:
        heartbeat_task.cancel()


@router.websocket("/ws/dags/{dag_id}")
async def dag_events(websocket: WebSocket, dag_id: str) -> None:
    """Streams TaskEvent for each node in this DAG, then a single
    DagCompletedEvent once every node has succeeded."""
    await _stream(websocket, dag_id)


@router.websocket("/ws/events")
async def global_events(websocket: WebSocket) -> None:
    """Streams every event across every task and DAG in the system —
    intended for a system-wide activity dashboard."""
    await _stream(websocket, None)