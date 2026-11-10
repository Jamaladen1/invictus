"""Test configuration.

Unit tests need nothing. Integration tests need PostgreSQL and Redis:

    INVICTUS_TEST_DATABASE_URL  (default postgresql+psycopg://invictus:invictus@localhost:5433/invictus_test)
    INVICTUS_TEST_REDIS_URL     (default redis://localhost:6380/15)

They are skipped automatically when those services are unreachable.
"""

import os
import threading
import time

import pytest

TEST_DB = os.environ.get("INVICTUS_TEST_DATABASE_URL",
                         "postgresql+psycopg://invictus:invictus@localhost:5433/invictus_test")
TEST_REDIS = os.environ.get("INVICTUS_TEST_REDIS_URL", "redis://localhost:6380/15")

# Must be set before invictus.config.get_settings() is first called.
os.environ.update({
    "INVICTUS_DATABASE_URL": TEST_DB,
    "INVICTUS_REDIS_URL": TEST_REDIS,
    "INVICTUS_API_KEYS": "dev-key:demo,other-key:acme",
    "INVICTUS_LEASE_SECONDS": "2",
    "INVICTUS_POLL_BLOCK_MS": "200",
    "INVICTUS_DISPATCHER_INTERVAL_SECONDS": "0.1",
    "INVICTUS_SWEEP_INTERVAL_SECONDS": "3600",
    "INVICTUS_LLM_PROVIDER": "fake",
    "INVICTUS_ENABLE_MOCK_TOOL": "true",
    "INVICTUS_FAULTS": "",
    "INVICTUS_SKIP_INIT_DB": "1",
})

API = {"X-API-Key": "dev-key"}


def _services_up() -> str | None:
    try:
        import psycopg
        import redis

        psycopg.connect(TEST_DB.replace("+psycopg", ""), connect_timeout=2).close()
        redis.Redis.from_url(TEST_REDIS, socket_connect_timeout=2).ping()
        return None
    except Exception as e:  # noqa: BLE001
        return f"{type(e).__name__}: {e}"


@pytest.fixture(scope="session")
def infra():
    reason = _services_up()
    if reason:
        pytest.skip(f"PostgreSQL/Redis unavailable: {reason}")
    from sqlalchemy import text

    from invictus.db import get_engine, init_db
    from invictus.models import Base

    with get_engine().begin() as c:
        Base.metadata.drop_all(c)
        for t in ("checkpoint_writes", "checkpoint_blobs", "checkpoints", "checkpoint_migrations"):
            c.execute(text(f"DROP TABLE IF EXISTS {t} CASCADE"))
    init_db()
    return True


@pytest.fixture
def clean(infra):
    from sqlalchemy import text

    from invictus.db import get_engine
    from invictus.queue import get_redis

    with get_engine().begin() as c:
        c.execute(text(
            "TRUNCATE workflow_definitions, workflow_runs, node_attempts, approval_events, outbox, "
            "audit_log, tool_executions, side_effects, checkpoints, checkpoint_writes, checkpoint_blobs CASCADE"))
    get_redis().flushdb()
    yield


@pytest.fixture
def client(clean):
    from fastapi.testclient import TestClient

    from invictus.api import app

    with TestClient(app) as c:
        c.headers.update(API)
        yield c


class Stack:
    """Dispatcher and/or worker running in background threads."""

    def __init__(self):
        self.threads: list[threading.Thread] = []
        self.stoppers = []

    def start_dispatcher(self):
        from invictus.dispatcher import Dispatcher

        d = Dispatcher()
        self._spawn(d.run, d.stop)

    def start_worker(self, concurrency: int = 2):
        from invictus.worker import Worker

        w = Worker(concurrency=concurrency)
        self._spawn(w.run, w.stop)

    def _spawn(self, target, stop):
        t = threading.Thread(target=target, daemon=True)
        t.start()
        self.threads.append(t)
        self.stoppers.append(stop)

    def stop(self):
        for s in self.stoppers:
            s()
        for t in self.threads:
            t.join(timeout=10)


@pytest.fixture
def stack(clean):
    s = Stack()
    yield s
    s.stop()


def wait_status(client, run_id: str, statuses: set[str], timeout: float = 30) -> dict:
    deadline = time.time() + timeout
    run = None
    while time.time() < deadline:
        run = client.get(f"/v1/runs/{run_id}").json()
        if run["status"] in statuses:
            return run
        time.sleep(0.1)
    raise AssertionError(f"run {run_id} stuck in {run and run['status']}: {run}")


def create_workflow(client, nodes, edges=(), name="wf") -> str:
    r = client.post("/v1/workflows", json={"name": name, "nodes": list(nodes), "edges": list(edges)})
    assert r.status_code == 201, r.text
    return r.json()["id"]


def start_run(client, wf_id: str, inp=None, key: str | None = None) -> dict:
    headers = {"Idempotency-Key": key} if key else {}
    r = client.post(f"/v1/workflows/{wf_id}/runs", json={"input": inp or {}}, headers=headers)
    assert r.status_code in (200, 201), r.text
    return r.json()
