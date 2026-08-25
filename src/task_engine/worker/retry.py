"""
Backoff policy for retried tasks. Kept separate from worker.py so the delay
math is unit-testable without spinning up asyncio or Redis, and so
alternative policies can be swapped into a WorkerPool without touching the
execution loop itself.
"""

from __future__ import annotations

import random
from typing import Optional

from task_engine.config import settings


class RetryPolicy:
    """
    Exponential backoff: delay = min(base * 2^(attempt-1), max), with
    optional jitter so a batch of tasks that failed together (e.g. a
    downstream API blip) don't all retry at the exact same instant and
    hammer it again the moment it recovers.
    """

    def __init__(
        self,
        base_seconds: Optional[float] = None,
        max_seconds: Optional[float] = None,
        jitter: bool = True,
    ) -> None:
        self.base_seconds = (
            base_seconds if base_seconds is not None else settings.retry_backoff_base_seconds
        )
        self.max_seconds = (
            max_seconds if max_seconds is not None else settings.retry_backoff_max_seconds
        )
        self.jitter = jitter

    def delay_for(self, attempt: int) -> float:
        """
        `attempt` is the retry count AFTER incrementing (1 for the first
        retry, 2 for the second, ...). Returns seconds to wait before the
        task is re-queued.
        """
        attempt = max(attempt, 1)
        raw = self.base_seconds * (2 ** (attempt - 1))
        delay = min(raw, self.max_seconds)
        if self.jitter:
            delay = random.uniform(delay * 0.5, delay)
        return delay