# Skein

**A distributed task execution engine, built from scratch in async Python.**
Priority scheduling. DAG-aware sub-task dependencies. Real-time execution visibility.
No Celery. No Airflow. Just `asyncio` + Redis and a clear model of how a task queue
actually works under the hood.

```
pip install skein-engine   # (placeholder — see Quickstart)
```

---

## Why this exists

Every team eventually needs to run work in the background — and reaches for Celery,
which drags in a broker abstraction, a results backend, and a decade of legacy design
decisions to do it. Or Airflow, which is built for scheduled batch DAGs, not
low-latency task dispatch.

**Skein is the queue you'd design if you started today, with `asyncio` as the
baseline instead of an afterthought.** It's small enough to read end-to-end in an
afternoon, and honest about what a task queue actually has to solve:

- How do you guarantee a task with dependencies doesn't run before its parents finish?
- How do you make "priority" mean something under load, not just a sort key nobody checks?
- How do you know what's happening *right now*, without polling a database?

This project answers those questions directly, in code, not in slides.

---

## What it does

| Capability | How |
|---|---|
| **Priority scheduling** | Tasks live in a Redis sorted set, scored by priority + submission time — O(log N) push/pop, no polling loop wasting cycles |
| **DAG-aware dependencies** | Submit a task graph, not just a task. Children are held until every parent reports `SUCCESS`, with cycle detection at submission time |
| **Async-native workers** | A pool of `asyncio` coroutines, not OS threads or forked processes — thousands of I/O-bound tasks in flight per worker |
| **Real-time monitoring** | Every state transition publishes to Redis pub/sub and streams over WebSocket — watch a DAG execute live, no refresh button |
| **Retry & backoff** | Configurable per-task retry policy with exponential backoff, without losing the task's place in the DAG |
| **Clean separation of concerns** | Domain logic (`core/`) has zero I/O and is fully unit-testable in isolation from Redis |

---

## Architecture at a glance

```
Client → API (FastAPI) → DAG Resolver → Priority Queue (Redis)
                               ↓                  ↓
                        Dependency Tracker ← Worker Pool (asyncio)
                               ↓                  ↓
                          Pub/Sub Events → WebSocket → Client (live status)
```

Five layers, each depending only on the ones before it:

```
core        → pure domain models (Task, DAG, state machine) — no I/O
queue       → Redis mechanics (priority queue, DAG resolver, result store)
worker      → asyncio execution loop + concurrency pool
scheduler   → orchestration: unlocks DAG children as parents complete
api         → FastAPI + WebSocket surface over the whole engine
```

Full breakdown of every module and how they call into each other is in
[`ARCHITECTURE.md`](./ARCHITECTURE.md).

---

## Quickstart

```bash
git clone https://github.com/<you>/skein.git
cd skein
docker compose up -d          # spins up Redis
pip install -r requirements.txt

# Terminal 1
python scripts/run_worker.py

# Terminal 2
python scripts/run_api.py
```

Submit a DAG:

```python
from task_engine.core.dag import DAG
from task_engine.worker.registry import task

@task("fetch_data")
async def fetch_data(url: str): ...

@task("transform")
async def transform(data: dict): ...

@task("load")
async def load(record: dict): ...

dag = DAG.from_edges([
    ("fetch_data", "transform"),
    ("transform", "load"),
])
```

```bash
curl -X POST localhost:8000/dags -d @dag.json
# then watch it execute:
wscat -c ws://localhost:8000/ws/dags/<dag_id>
```

---

## Design decisions worth knowing about

- **Redis sorted sets over Redis lists** for the priority queue — `O(log N)`
  insertion with priority as score, versus `O(N)` scans or maintaining N separate
  list keys per priority level.
- **DAG state lives in the result store, not in memory** — the scheduler is
  stateless and can be killed and restarted without losing track of which nodes
  have unlocked.
- **Pub/sub for status, not the results store** — polling `result_store` for
  "is it done yet" doesn't scale past a handful of watchers; pub/sub does.
- **Workers are coroutines, not processes** — this engine is built for I/O-bound
  task graphs (API calls, DB writes, file I/O). CPU-bound work should be dispatched
  to a process pool from inside a task, not fight the event loop.

---

## Roadmap

- [ ] Dead-letter queue for exhausted retries
- [ ] Per-DAG concurrency limits (not just global worker count)
- [ ] Task result TTL + archival to Postgres
- [ ] Horizontal worker scaling across hosts (currently single-host worker pool)
- [ ] Prometheus metrics endpoint

---

## Tech stack

`Python 3.12` · `asyncio` · `Redis` (sorted sets, pub/sub) · `FastAPI` · `Pydantic` ·
`WebSockets` · `pytest` + `pytest-asyncio`

---

## Status

Actively developed as a systems-design deep dive into how production task queues
work under the hood — priority scheduling, dependency graphs, and real-time
observability, built from primitives rather than imported off the shelf.

Issues, questions, and PRs welcome.