import atexit
from contextlib import contextmanager
from functools import lru_cache
from typing import Iterator

from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker

from .config import get_settings
from .models import Base


@lru_cache
def get_engine() -> Engine:
    return create_engine(get_settings().database_url, pool_pre_ping=True, pool_size=10, max_overflow=10)


@lru_cache
def _session_factory() -> sessionmaker[Session]:
    return sessionmaker(bind=get_engine(), expire_on_commit=False)


@contextmanager
def session_scope() -> Iterator[Session]:
    """A transaction: commits on success, rolls back on error."""
    session = _session_factory()()
    try:
        yield session
        session.commit()
    except BaseException:
        session.rollback()
        raise
    finally:
        session.close()


def _libpq_url() -> str:
    # SQLAlchemy URL -> plain libpq URL for psycopg / LangGraph.
    return get_settings().database_url.replace("postgresql+psycopg://", "postgresql://", 1)


@lru_cache
def get_checkpoint_pool() -> ConnectionPool:
    pool = ConnectionPool(
        _libpq_url(),
        min_size=1,
        max_size=10,
        kwargs={"autocommit": True, "prepare_threshold": 0, "row_factory": dict_row},
        open=True,
    )
    atexit.register(pool.close, timeout=2)
    return pool


def init_db() -> None:
    """Create tables (idempotent). A production deployment would use migrations instead."""
    from langgraph.checkpoint.postgres import PostgresSaver

    engine = get_engine()
    with engine.begin() as conn:
        # Serialise concurrent initialisers (api + workers starting together).
        conn.execute(text("SELECT pg_advisory_xact_lock(424242)"))
        Base.metadata.create_all(conn)
    with get_checkpoint_pool().connection() as conn:
        conn.execute("SELECT pg_advisory_lock(424243)")
        try:
            PostgresSaver(conn).setup()
        finally:
            conn.execute("SELECT pg_advisory_unlock(424243)")
