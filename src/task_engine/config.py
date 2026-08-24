"""
task_engine/config.py

Centralized settings for the engine. Every other module (queue, worker,
scheduler, api) imports `settings` from here instead of reading environment
variables directly — keeps configuration in one auditable place and makes
tests trivial to override via `Settings(**overrides)`.

Values are loaded from environment variables / a `.env` file, using
pydantic-settings so they're validated and typed at startup rather than
failing halfway through a task run.
"""

from functools import lru_cache
from typing import Literal

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        env_prefix="TASK_ENGINE_",
        case_sensitive=False,
        extra="ignore",
    )

    # ------------------------------------------------------------------
    # Redis — backs the priority queue, result store, DAG state, pub/sub
    # ------------------------------------------------------------------
    redis_url: str = Field(
        default="redis://localhost:6379/0",
        description="Connection string for redis.asyncio.from_url()",
    )
    redis_max_connections: int = Field(
        default=50,
        ge=1,
        description="Cap on the shared connection pool used by redis_client.py",
    )
    redis_socket_timeout: float = Field(
        default=5.0,
        description="Seconds before a Redis command times out",
    )

    # Key namespacing — lets multiple environments/tenants share one Redis
    # instance without colliding (queue/priority_queue.py, result_store.py,
    # dag_resolver.py all prefix their keys with this).
    key_prefix: str = Field(default="skein", description="Namespace prefix for all Redis keys")

    # ------------------------------------------------------------------
    # Priority queue
    # ------------------------------------------------------------------
    # Lower number == higher priority (0 = urgent). Enforced as the score
    # in the Redis sorted set; queue/priority_queue.py clamps to this range.
    min_priority: int = Field(default=0, description="Highest allowed priority (0 = most urgent)")
    max_priority: int = Field(default=9, description="Lowest allowed priority")
    default_priority: int = Field(default=5)

    @field_validator("max_priority")
    @classmethod
    def _max_gte_min(cls, v: int, info) -> int:
        min_p = info.data.get("min_priority", 0)
        if v < min_p:
            raise ValueError("max_priority must be >= min_priority")
        return v

    # ------------------------------------------------------------------
    # Worker pool
    # ------------------------------------------------------------------
    worker_concurrency: int = Field(
        default=10,
        ge=1,
        description="Number of concurrent asyncio task coroutines per worker process",
    )
    worker_poll_interval: float = Field(
        default=0.1,
        description="Seconds to sleep between empty-queue polls (worker.py)",
    )
    task_timeout_seconds: float = Field(
        default=300.0,
        description="Hard ceiling on a single task's execution time before it's killed",
    )

    # ------------------------------------------------------------------
    # Retry / backoff (worker/retry.py)
    # ------------------------------------------------------------------
    max_retries: int = Field(default=3, ge=0)
    retry_backoff_base_seconds: float = Field(
        default=1.0,
        description="Base for exponential backoff: delay = base * 2^attempt",
    )
    retry_backoff_max_seconds: float = Field(
        default=60.0,
        description="Ceiling on backoff delay regardless of attempt count",
    )

    # ------------------------------------------------------------------
    # Result / state retention
    # ------------------------------------------------------------------
    result_ttl_seconds: int = Field(
        default=86_400,
        description="How long a completed task's result stays in result_store before expiry",
    )

    # ------------------------------------------------------------------
    # Scheduler / DAG resolver
    # ------------------------------------------------------------------
    scheduler_tick_seconds: float = Field(
        default=0.5,
        description="How often scheduler.py checks for newly-unlocked DAG nodes",
    )
    max_dag_depth: int = Field(
        default=100,
        description="Safety limit — DAGs deeper than this are rejected at submission",
    )

    # ------------------------------------------------------------------
    # API / WebSocket
    # ------------------------------------------------------------------
    api_host: str = Field(default="0.0.0.0")
    api_port: int = Field(default=8000)
    websocket_heartbeat_seconds: float = Field(
        default=15.0,
        description="Ping interval to keep WebSocket connections alive through proxies",
    )
    cors_origins: list[str] = Field(default_factory=lambda: ["*"])

    # ------------------------------------------------------------------
    # Observability
    # ------------------------------------------------------------------
    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR"] = Field(default="INFO")
    metrics_enabled: bool = Field(default=False)


@lru_cache
def get_settings() -> Settings:
    """
    Cached accessor so `Settings()` — which reads env vars and the .env file —
    only runs once per process. Import `settings` (below) for the common case;
    use `get_settings()` directly in tests when you need to bypass the cache
    (e.g. `get_settings.cache_clear()` between test cases with different env).
    """
    return Settings()


# Module-level singleton — the import most other modules will actually use:
#   from task_engine.config import settings
settings = get_settings() 