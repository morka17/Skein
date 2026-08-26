"""

    POST   /tasks         submit a standalone task (no DAG)
    GET    /tasks/{id}     current state/result
    DELETE /tasks/{id}     cancel, only while still QUEUED

Every handler reads its dependencies (scheduler, results, queue) off
`request.app.state`, set up once in api/main.py's lifespan — routes stay
thin and don't construct their own Redis connections.
"""

from __future__ import annotations

from fastapi import APIRouter, HTTPException, Request

from task_engine.api.schemas import TaskResponse, TaskSubmitRequest
from task_engine.core.states import TaskState
from task_engine.core.task import Task

router = APIRouter(prefix="/tasks", tags=["tasks"])


@router.post("", response_model=TaskResponse, status_code=201)
async def submit_task(body: TaskSubmitRequest, request: Request) -> TaskResponse:
    scheduler = request.app.state.scheduler

    # Only pass fields the client actually set — letting Task's own
    # defaults (settings.default_priority, settings.max_retries) apply
    # otherwise, rather than re-deciding those defaults here.
    kwargs: dict = {"name": body.name, "payload": body.payload}
    if body.priority is not None:
        kwargs["priority"] = body.priority
    if body.max_retries is not None:
        kwargs["max_retries"] = body.max_retries

    task = Task(**kwargs)
    task = await scheduler.submit_task(task)
    return TaskResponse.from_task(task)


@router.get("/{task_id}", response_model=TaskResponse)
async def get_task(task_id: str, request: Request) -> TaskResponse:
    results = request.app.state.results
    task = await results.get(task_id)
    if task is None:
        raise HTTPException(status_code=404, detail=f"task {task_id} not found")
    return TaskResponse.from_task(task)


@router.delete("/{task_id}", status_code=204)
async def cancel_task(task_id: str, request: Request) -> None:
    """
    Only cancels a task still sitting in the queue — once a worker has
    claimed it (RUNNING), there's no safe way to interrupt arbitrary user
    code mid-execution, so this returns 409 rather than pretending to
    cancel something already in flight.
    """
    results = request.app.state.results
    queue = request.app.state.queue

    task = await results.get(task_id)
    if task is None:
        raise HTTPException(status_code=404, detail=f"task {task_id} not found")

    if task.state != TaskState.QUEUED:
        raise HTTPException(
            status_code=409,
            detail=f"task {task_id} is {task.state.value} — only QUEUED tasks can be cancelled",
        )

    removed = await queue.remove(task_id)
    if not removed:
        # A worker popped it between our GET and this call — it's about
        # to start running; there's nothing left in the queue to remove.
        raise HTTPException(
            status_code=409, detail=f"task {task_id} was claimed by a worker before it could be cancelled"
        )

    task.mark_cancelled()
    await results.save(task)