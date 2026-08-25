"""
Maps a Task's `name` string to the actual coroutine that should run for it.
This indirection is *why* core.Task can be serialized into Redis without
ever containing code — a worker process just needs this registry populated
(by importing whatever module defines the @task functions) before it starts
pulling from the queue.
"""

from __future__ import annotations

import inspect
from typing import Any, Awaitable, Callable, Optional

TaskFunc = Callable[..., Awaitable[Any]]


class UnknownRegisteredTaskError(Exception):
    """Raised when a Task references a name with no matching @task function."""


class TaskRegistry:
    def __init__(self) -> None:
        self._funcs: dict[str, TaskFunc] = {}

    def register(self, name: str, func: TaskFunc) -> None:
        if not inspect.iscoroutinefunction(func):
            raise TypeError(
                f"task '{name}' ({func!r}) must be an async def function — "
                f"worker.py awaits it directly, sync tasks aren't supported "
                f"(wrap blocking calls in asyncio.to_thread inside your task instead)"
            )
        if name in self._funcs:
            raise ValueError(f"task name '{name}' is already registered")
        self._funcs[name] = func

    def get(self, name: str) -> TaskFunc:
        try:
            return self._funcs[name]
        except KeyError:
            raise UnknownRegisteredTaskError(
                f"no task registered under name '{name}' — make sure the module "
                f"defining it was imported before the worker process started"
            ) from None

    def __contains__(self, name: str) -> bool:
        return name in self._funcs

    def names(self) -> list[str]:
        return list(self._funcs)


# Module-level singleton — the registry every worker process shares by default.
default_registry = TaskRegistry()


def task(
    name: str, *, registry: Optional[TaskRegistry] = None
) -> Callable[[TaskFunc], TaskFunc]:
    """
    Decorator that registers an async function as a runnable task:

        @task("send_email")
        async def send_email(to: str, subject: str) -> None:
            ...

    A Task's `payload` dict is unpacked as **kwargs when worker.py calls
    this function — dict keys must match the function's parameter names.
    """

    def decorator(func: TaskFunc) -> TaskFunc:
        (registry or default_registry).register(name, func)
        return func

    return decorator