"""
Optional durable audit trail beyond Redis. Redis (queue/result_store.py)
is authoritative for LIVE task state and expires terminal records after
settings.result_ttl_seconds — this module is for anyone who wants task
history to outlive that TTL: compliance, analytics, or debugging a DAG
that ran three weeks ago. Nothing else in the engine hard-depends on this
module; it's wired in opt-in, typically from a monitoring/events.py
subscriber that calls PersistenceStore.record() on terminal states.

Requires the `postgres` extra: pip install -e ".[postgres]"
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Optional

from sqlalchemy import JSON, DateTime, Integer, String, select
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from task_engine.config import settings
from task_engine.core.task import Task

logger = logging.getLogger(__name__)


class Base(DeclarativeBase):
    pass


class TaskRecord(Base):
    """
    One row per task, upserted on terminal states (SUCCESS/FAILED/
    CANCELLED). Deliberately denormalized — this is a read-mostly audit
    log, not a live coordination store (Redis owns that job), so it
    doesn't need a separate DAG table: dag_id is just a column here,
    queryable via list_by_dag().
    """

    __tablename__ = "task_records"

    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    name: Mapped[str] = mapped_column(String(255), index=True)
    dag_id: Mapped[Optional[str]] = mapped_column(String(36), index=True, nullable=True)

    priority: Mapped[int] = mapped_column(Integer)
    state: Mapped[str] = mapped_column(String(20), index=True)
    retries: Mapped[int] = mapped_column(Integer, default=0)

    payload: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)
    result: Mapped[Optional[Any]] = mapped_column(JSON, nullable=True)
    error: Mapped[Optional[str]] = mapped_column(String, nullable=True)

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    started_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    completed_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)


_engine: Optional[AsyncEngine] = None
_session_factory: Optional[async_sessionmaker[AsyncSession]] = None


def _require_database_url() -> str:
    if not settings.database_url:
        raise RuntimeError(
            "settings.database_url is not set — persistence/db.py requires "
            "TASK_ENGINE_DATABASE_URL, e.g. "
            "postgresql+asyncpg://user:pass@host/db"
        )
    return settings.database_url


def get_engine() -> AsyncEngine:
    """Process-wide async engine, created lazily on first use — mirrors
    queue/redis_client.py's get_redis() pattern."""
    global _engine
    if _engine is None:
        _engine = create_async_engine(_require_database_url(), pool_pre_ping=True)
    return _engine


def get_session_factory() -> async_sessionmaker[AsyncSession]:
    global _session_factory
    if _session_factory is None:
        _session_factory = async_sessionmaker(get_engine(), expire_on_commit=False)
    return _session_factory


async def init_models() -> None:
    """
    Creates task_records if it doesn't already exist. Call once at startup
    (scripts/run_api.py) if persistence is enabled — idempotent, safe on
    every boot. For real production schema changes, prefer a migration
    tool (Alembic) over this; it exists to make local dev/demo setup a
    single call rather than a manual `CREATE TABLE`.
    """
    engine = get_engine()
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    logger.info("persistence: task_records table ready")


async def close_db() -> None:
    """Call during application shutdown to dispose the connection pool
    cleanly — mirrors queue/redis_client.py's close_redis()."""
    global _engine, _session_factory
    if _engine is not None:
        await _engine.dispose()
    _engine = None
    _session_factory = None


class PersistenceStore:
    """
    Durable counterpart to queue/result_store.py. Call `record(task)`
    wherever you want a task's history to survive Redis's TTL — typically
    from a monitoring/events.py subscriber, and typically only on terminal
    states (SUCCESS/FAILED/CANCELLED). Recording every RUNNING/QUEUED
    transition here would turn an audit log into a write-heavy live store,
    which is exactly the job Redis already does better.
    """

    def __init__(
        self, session_factory: Optional[async_sessionmaker[AsyncSession]] = None
    ) -> None:
        self._session_factory = session_factory or get_session_factory()

    async def record(self, task: Task) -> None:
        """Upsert: inserts a new row the first time a task is seen, updates
        it on every subsequent call (e.g. RETRY -> eventual FAILED)."""
        async with self._session_factory() as session:
            existing = await session.get(TaskRecord, task.id)
            if existing is None:
                session.add(self._to_record(task))
            else:
                self._apply(existing, task)
            await session.commit()

    async def get(self, task_id: str) -> Optional[TaskRecord]:
        async with self._session_factory() as session:
            return await session.get(TaskRecord, task_id)

    async def list_by_dag(self, dag_id: str) -> list[TaskRecord]:
        """Full history for one DAG run, oldest first — useful for a
        post-mortem view once a DAG has long since expired out of Redis."""
        async with self._session_factory() as session:
            result = await session.execute(
                select(TaskRecord)
                .where(TaskRecord.dag_id == dag_id)
                .order_by(TaskRecord.created_at)
            )
            return list(result.scalars().all())

    @staticmethod
    def _to_record(task: Task) -> TaskRecord:
        return TaskRecord(
            id=task.id,
            name=task.name,
            dag_id=task.dag_id,
            priority=task.priority,
            state=task.state.value,
            retries=task.retries,
            payload=task.payload,
            result=task.result,
            error=task.error,
            created_at=task.created_at,
            updated_at=task.updated_at,
            started_at=task.started_at,
            completed_at=task.completed_at,
        )

    @staticmethod
    def _apply(record: TaskRecord, task: Task) -> None:
        record.state = task.state.value
        record.retries = task.retries
        record.result = task.result
        record.error = task.error
        record.updated_at = task.updated_at
        record.started_at = task.started_at
        record.completed_at = task.completed_at