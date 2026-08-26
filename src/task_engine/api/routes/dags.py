"""
task_engine/api/routes/dags.py

    POST /dags        submit a DAG (nodes + edges); queues root nodes
    GET  /dags/{id}    status of every node, plus overall completion
"""

from __future__ import annotations

from fastapi import APIRouter, HTTPException, Request

from task_engine.api.schemas import (
    DagStatusResponse,
    DagSubmitRequest,
    DagSubmitResponse,
    TaskResponse,
)
from task_engine.core.dag import DAG
from task_engine.core.exceptions import (
    CycleDetectedError,
    DagDepthExceededError,
    UnknownTaskError,
)
from task_engine.core.task import Task

router = APIRouter(prefix="/dags", tags=["dags"])


@router.post("", response_model=DagSubmitResponse, status_code=201)
async def submit_dag(body: DagSubmitRequest, request: Request) -> DagSubmitResponse:
    scheduler = request.app.state.scheduler

    tasks = []
    for node in body.nodes:
        kwargs: dict = {"id": node.id, "name": node.name, "payload": node.payload}
        if node.priority is not None:
            kwargs["priority"] = node.priority
        if node.max_retries is not None:
            kwargs["max_retries"] = node.max_retries
        tasks.append(Task(**kwargs))

    try:
        # All structural validation — cycles, depth, unknown edge
        # endpoints — happens inside DAG.from_tasks(); this route only
        # translates its exceptions into HTTP status codes.
        dag = DAG.from_tasks(tasks, body.edges)
    except (UnknownTaskError, CycleDetectedError, DagDepthExceededError) as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    queued = await scheduler.submit_dag(dag)
    return DagSubmitResponse(
        dag_id=dag.id,
        task_ids=list(dag.tasks.keys()),
        queued=[t.id for t in queued],
    )


@router.get("/{dag_id}", response_model=DagStatusResponse)
async def get_dag_status(dag_id: str, request: Request) -> DagStatusResponse:
    resolver = request.app.state.resolver
    results = request.app.state.results

    task_ids = await resolver.list_task_ids(dag_id)
    if task_ids is None:
        raise HTTPException(status_code=404, detail=f"dag {dag_id} not found")

    tasks = await results.get_many(task_ids)
    complete = await resolver.is_complete(dag_id)

    return DagStatusResponse(
        dag_id=dag_id,
        complete=complete,
        tasks=[TaskResponse.from_task(t) for t in tasks.values()],
    )