"""
task_engine.scheduler — orchestration: turns task-completion events into
DAG progress, plus a reconciliation loop as a safety net for missed events.

Depends on task_engine.queue (DAGResolver, PriorityQueue, ResultStore) and
task_engine.core (Task, DAG). api/ depends on this package; this package
does not import api/ or monitoring/ — see dependency_tracker.py's
`downstream` hook for how those plug in instead.
"""

from task_engine.scheduler.dependency_tracker import DependencyTracker
from task_engine.scheduler.scheduler import Scheduler

__all__ = ["Scheduler", "DependencyTracker"]