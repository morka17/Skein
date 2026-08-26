"""
Liveness/readiness check and Prometheus metrics exposition. No auth here —
in production, front /metrics separately (e.g. restrict at the ingress
level) rather than relying on this router for access control.
"""

from __future__ import annotations

from fastapi import APIRouter, Response

from task_engine.monitoring.metrics import metrics_endpoint
from task_engine.queue.redis_client import ping

router = APIRouter(tags=["health"])


@router.get("/health")
async def health() -> dict[str, bool]:
    """Reports Redis reachability — the one dependency this whole engine
    can't function without. Used by orchestrators (k8s liveness/readiness
    probes, load balancer health checks) to decide whether to route
    traffic here."""
    redis_ok = await ping()
    return {"ok": redis_ok, "redis": redis_ok}


@router.get("/metrics")
async def metrics() -> Response:
    """Prometheus exposition format. Returns a placeholder comment instead
    of real metrics if settings.metrics_enabled is False or the `metrics`
    extra isn't installed — see monitoring/metrics.py."""
    return Response(
        content=metrics_endpoint(),
        media_type="text/plain; version=0.0.4; charset=utf-8",
    )