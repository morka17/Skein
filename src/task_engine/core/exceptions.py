"""
Domain-level exceptions. Kept separate from queue/worker/api exceptions so
`core` stays free of any dependency on Redis, FastAPI, etc. These are caught
and translated (e.g. to HTTP 4xx responses) at the api/ layer — core code
itself should never know an HTTP status code exists.
"""


class TaskEngineError(Exception):
    """Base class for all engine-raised errors."""


class InvalidTransitionError(TaskEngineError):
    """Raised when a task's state machine is asked to make an illegal jump
    (e.g. PENDING -> SUCCESS, skipping RUNNING)."""


class InvalidPriorityError(TaskEngineError):
    """Raised when a task priority falls outside settings.min/max_priority."""


class CycleDetectedError(TaskEngineError):
    """Raised at DAG construction time when the dependency graph isn't
    actually acyclic."""


class DagDepthExceededError(TaskEngineError):
    """Raised when a submitted DAG's longest path exceeds settings.max_dag_depth."""


class UnknownTaskError(TaskEngineError):
    """Raised when a DAG edge references a task id that isn't in the graph."""