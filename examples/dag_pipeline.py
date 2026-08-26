"""
A small fan-out/fan-in DAG: fetch -> {transform_a, transform_b} -> load.
transform_a and transform_b both become eligible the moment fetch
succeeds and run concurrently; load only starts once BOTH have
succeeded. This is the "sub-task DAG dependency tree" requirement made
concrete — DAG.ready_nodes() (core/dag.py) is what enforces the fan-in.

Submit it once the API and a worker (with this module imported) are
running:

    curl -X POST localhost:8000/dags -H 'content-type: application/json' -d '{
      "nodes": [
        {"id": "fetch",       "name": "fetch_data",  "payload": {"url": "https://example.com/data"}},
        {"id": "transform_a", "name": "transform",   "payload": {"field": "a"}},
        {"id": "transform_b", "name": "transform",   "payload": {"field": "b"}},
        {"id": "load",        "name": "load_result", "payload": {}}
      ],
      "edges": [
        ["fetch", "transform_a"],
        ["fetch", "transform_b"],
        ["transform_a", "load"],
        ["transform_b", "load"]
      ]
    }'

Then watch it execute live:  wscat -c ws://localhost:8000/ws/dags/<dag_id>
"""

from __future__ import annotations

import asyncio
import random

from task_engine.worker.registry import task


@task("fetch_data")
async def fetch_data(url: str = "https://example.com/data") -> dict:
    """Stands in for a real network call — worker.py awaits this under
    settings.task_timeout_seconds exactly like it would a real one."""
    await asyncio.sleep(0.5)
    return {"url": url, "rows": [1, 2, 3, 4, 5]}


@task("transform")
async def transform(field: str) -> dict:
    """Runs once for transform_a and once for transform_b — both share
    `fetch` as their only parent, so a single call to
    DAGResolver.on_task_success() (triggered by fetch's SUCCESS) unlocks
    and queues both of them together."""
    await asyncio.sleep(random.uniform(0.2, 0.8))
    return {"field": field, "transformed": True}


@task("load_result")
async def load_result() -> str:
    """Only becomes eligible once BOTH transform_a and transform_b have
    reached SUCCESS — DAG.ready_nodes() requires every parent listed for
    a node to have succeeded before that node is unlocked, which is
    exactly the fan-in this task demonstrates."""
    await asyncio.sleep(0.3)
    return "loaded"