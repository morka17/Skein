"""
API / control-plane process entrypoint:  python scripts/run_api.py

Starts the FastAPI app (task_engine.api.main:app) under uvicorn —
submission endpoints, DAG status, health/metrics, and the WebSocket event
stream all live here.

This process never executes a task itself — see scripts/run_worker.py for
that. Running them as separate processes (and separately scalable) is
what makes "distributed" mean something rather than just being a slogan.
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import uvicorn

from task_engine.config import settings


def run() -> None:
    uvicorn.run(
        "task_engine.api.main:app",
        host=settings.api_host,
        port=settings.api_port,
        log_level=settings.log_level.lower(),
        reload=False,
    )


if __name__ == "__main__":
    run()