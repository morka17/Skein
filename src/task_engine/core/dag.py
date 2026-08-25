"""
task_engine/core/dag.py

The DAG domain model: a set of Task nodes plus directed edges expressing
"child depends on parent". Responsible for structural validation (cycle
detection, depth limits) and pure graph queries (topological order, which
nodes are ready to run). This class does NOT talk to Redis —
queue/dag_resolver.py is the I/O layer that calls `ready_nodes()` against
live task state pulled from result_store.py.
"""

from __future__ import annotations

import uuid
from collections import defaultdict, deque
from typing import Iterable

from pydantic import BaseModel, Field

from task_engine.config import settings
from task_engine.core.exceptions import (
    CycleDetectedError,
    DagDepthExceededError,
    UnknownTaskError,
)
from task_engine.core.task import Task


class DAG(BaseModel):
    """
    `tasks` is keyed by Task.id. `edges` is a list of (parent_id, child_id)
    pairs — a child only becomes eligible to run once every parent listed
    in its incoming edges has reached SUCCESS.
    """

    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    tasks: dict[str, Task] = Field(default_factory=dict)
    edges: list[tuple[str, str]] = Field(default_factory=list)

    # ------------------------------------------------------------------
    # Construction
    # ------------------------------------------------------------------

    @classmethod
    def from_tasks(cls, tasks: Iterable[Task], edges: list[tuple[str, str]]) -> "DAG":
        """
        Build and fully validate a DAG in one step: every edge endpoint
        must reference a known task, the graph must be acyclic, and its
        depth must stay within settings.max_dag_depth. Also stamps
        `dag_id` and `depends_on` onto each task so the resolver has
        everything it needs from the task record alone, without re-walking
        the graph on every lookup.

        Raises UnknownTaskError, CycleDetectedError, or DagDepthExceededError
        on invalid input — callers (typically api/routes/dags.py) should
        catch these and translate to a 400 response.
        """
        dag_id = str(uuid.uuid4())
        task_map = {t.id: t for t in tasks}

        for parent_id, child_id in edges:
            if parent_id not in task_map or child_id not in task_map:
                raise UnknownTaskError(
                    f"edge ({parent_id} -> {child_id}) references a task not in this DAG"
                )

        dag = cls(id=dag_id, tasks=task_map, edges=edges)
        dag._detect_cycles()  # raises CycleDetectedError

        depth = dag._max_depth()
        if depth > settings.max_dag_depth:
            raise DagDepthExceededError(
                f"DAG depth {depth} exceeds settings.max_dag_depth={settings.max_dag_depth}"
            )

        for task in dag.tasks.values():
            task.dag_id = dag_id
            task.depends_on = dag.parents_of(task.id)

        return dag

    # ------------------------------------------------------------------
    # Graph structure helpers
    # ------------------------------------------------------------------

    def _adjacency(self) -> dict[str, list[str]]:
        """parent_id -> [child_id, ...]"""
        children: dict[str, list[str]] = defaultdict(list)
        for parent_id, child_id in self.edges:
            children[parent_id].append(child_id)
        return children

    def _reverse_adjacency(self) -> dict[str, list[str]]:
        """child_id -> [parent_id, ...]"""
        parents: dict[str, list[str]] = defaultdict(list)
        for parent_id, child_id in self.edges:
            parents[child_id].append(parent_id)
        return parents

    def parents_of(self, task_id: str) -> list[str]:
        return self._reverse_adjacency().get(task_id, [])

    def children_of(self, task_id: str) -> list[str]:
        return self._adjacency().get(task_id, [])

    def root_nodes(self) -> list[str]:
        """Tasks with no parents — eligible to run the instant the DAG starts."""
        has_parent = {child for _, child in self.edges}
        return [tid for tid in self.tasks if tid not in has_parent]

    def leaf_nodes(self) -> list[str]:
        """Tasks with no children — completion of all of these means the DAG is done."""
        has_child = {parent for parent, _ in self.edges}
        return [tid for tid in self.tasks if tid not in has_child]

    # ------------------------------------------------------------------
    # Validation
    # ------------------------------------------------------------------

    def _detect_cycles(self) -> None:
        """DFS with white/gray/black coloring — a gray-to-gray edge is a
        back-edge, i.e. a cycle. O(V + E), runs once at construction time."""
        WHITE, GRAY, BLACK = 0, 1, 2
        color = {tid: WHITE for tid in self.tasks}
        children = self._adjacency()

        def visit(node: str) -> None:
            color[node] = GRAY
            for child in children.get(node, []):
                if color[child] == GRAY:
                    raise CycleDetectedError(f"cycle detected in DAG: back-edge {node} -> {child}")
                if color[child] == WHITE:
                    visit(child)
            color[node] = BLACK

        for task_id in self.tasks:
            if color[task_id] == WHITE:
                visit(task_id)

    def _max_depth(self) -> int:
        """Longest path from any root to any leaf, counted in nodes.
        Memoized DFS — safe to call only after _detect_cycles() has passed."""
        children = self._adjacency()
        memo: dict[str, int] = {}

        def depth_from(node: str) -> int:
            if node in memo:
                return memo[node]
            kids = children.get(node, [])
            memo[node] = 1 if not kids else 1 + max(depth_from(c) for c in kids)
            return memo[node]

        if not self.tasks:
            return 0
        return max(depth_from(tid) for tid in self.tasks)

    # ------------------------------------------------------------------
    # Scheduling queries — used by scheduler/dependency_tracker.py
    # ------------------------------------------------------------------

    def topological_order(self) -> list[str]:
        """Kahn's algorithm. Assumes the DAG was built via from_tasks()
        (i.e. already known to be acyclic) — the length check below is a
        defensive guard, not the primary cycle check."""
        in_degree = {tid: 0 for tid in self.tasks}
        children = self._adjacency()
        for _parent_id, child_id in self.edges:
            in_degree[child_id] += 1

        queue = deque([tid for tid, deg in in_degree.items() if deg == 0])
        order: list[str] = []

        while queue:
            node = queue.popleft()
            order.append(node)
            for child in children.get(node, []):
                in_degree[child] -= 1
                if in_degree[child] == 0:
                    queue.append(child)

        if len(order) != len(self.tasks):
            raise CycleDetectedError("topological sort failed — graph contains a cycle")
        return order

    def ready_nodes(self, succeeded_ids: set[str]) -> list[str]:
        """
        Every task whose parents have ALL succeeded, and which hasn't
        itself succeeded yet. dependency_tracker.py calls this each time a
        task completes, to decide what to unlock and push to
        priority_queue.py next.
        """
        ready = []
        for task_id in self.tasks:
            if task_id in succeeded_ids:
                continue
            parents = self.parents_of(task_id)
            if all(p in succeeded_ids for p in parents):
                ready.append(task_id)
        return ready

    def is_complete(self, succeeded_ids: set[str]) -> bool:
        """True once every node in the DAG has reached SUCCESS. Used to
        fire the DagCompleted event in monitoring/events.py."""
        return succeeded_ids.issuperset(self.tasks.keys())