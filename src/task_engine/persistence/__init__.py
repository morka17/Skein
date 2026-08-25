"""
task_engine.persistence — optional durable audit trail (Postgres) beyond
Redis's result_ttl_seconds. Nothing else in the engine imports this
package; it's opt-in, typically wired up from a monitoring/events.py
subscriber that calls PersistenceStore.record() on terminal task states.

Requires the `postgres` extra: pip install -e ".[postgres]"
Unused unless settings.database_url is set — see task_engine.config.
"""

from task_engine.persistence.db import (
    Base,
    PersistenceStore,
    TaskRecord,
    close_db,
    get_engine,
    get_session_factory,
    init_models,
)

__all__ = [
    "Base",
    "TaskRecord",
    "PersistenceStore",
    "get_engine",
    "get_session_factory",
    "init_models",
    "close_db",
]