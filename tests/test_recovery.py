"""Fault injection: kill a real worker process at each execution boundary, then let a
healthy worker take over and check the run completes with the right side-effect counts.

Boundary            physical executions   visible side effects   abandoned attempts
before_execute      1                     1                      1
after_side_effect   2 (at-least-once)     1 (idempotency key)    1
after_persist       1 (result reused)     1                      0
"""

import os
import subprocess
import sys

import pytest
from sqlalchemy import func, select

from conftest import create_workflow, start_run, wait_status

pytestmark = pytest.mark.integration

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def crashing_worker(fault: str) -> subprocess.CompletedProcess:
    env = {**os.environ, "INVICTUS_FAULTS": fault, "LOG_LEVEL": "WARNING"}
    return subprocess.run([sys.executable, "-m", "invictus.worker", "--concurrency", "1"],
                          env=env, cwd=ROOT, capture_output=True, text=True, timeout=60)


@pytest.mark.parametrize("point, executions, abandoned", [
    ("before_execute", 1, 1),
    ("after_side_effect", 2, 1),
    ("after_persist", 1, 0),
])
def test_worker_crash_recovery(client, stack, point, executions, abandoned):
    from invictus.db import session_scope
    from invictus.models import AuditLog, SideEffect, ToolExecution

    stack.start_dispatcher()
    wf = create_workflow(client, [
        {"id": "prep", "type": "mock", "config": {"output": "p"}},
        {"id": "charge", "type": "mock", "config": {"output": "{{nodes.prep.value}}", "side_effect": True}},
        {"id": "notify", "type": "mock", "config": {"output": "{{nodes.charge.value}}"}},
    ], [{"source": "prep", "target": "charge"}, {"source": "charge", "target": "notify"}])
    run_id = start_run(client, wf)["id"]

    proc = crashing_worker(f"charge:{point}")
    assert proc.returncode == 137, proc.stderr[-2000:]
    assert client.get(f"/v1/runs/{run_id}").json()["status"] == "RUNNING"

    stack.start_worker(concurrency=1)  # healthy worker reclaims after the lease expires
    run = wait_status(client, run_id, {"SUCCEEDED", "FAILED", "REJECTED", "CANCELLED"}, timeout=30)

    assert run["status"] == "SUCCEEDED", run
    assert run["recovery_count"] == 1
    assert run["output"]["result"]["notify"]["value"] == "p"
    charge = next(s for s in run["steps"] if s["node_id"] == "charge")
    assert charge["abandoned_attempts"] == abandoned
    prep = next(s for s in run["steps"] if s["node_id"] == "prep")
    assert prep["attempts"] == 1, "completed upstream node was re-executed"
    with session_scope() as s:
        audit = s.scalars(select(AuditLog).where(AuditLog.run_id == run_id, AuditLog.action == "run.recovered")).all()
        assert len(audit) == 1
        assert audit[0].detail["recovery_count"] == 1
        assert audit[0].detail["previous_owner"] and audit[0].detail["previous_owner"] != audit[0].actor_id
        assert len(audit[0].detail["abandoned_attempts"]) == abandoned
        assert s.scalar(select(func.count()).select_from(ToolExecution)) == executions
        assert s.scalar(select(func.count()).select_from(SideEffect)) == 1


def test_poison_run_fails_after_recovery_budget(client, stack):
    """A node that kills every worker must not crash-loop forever."""
    from invictus.config import get_settings

    settings = get_settings()
    old = settings.max_run_recoveries
    settings.max_run_recoveries = 1
    try:
        stack.start_dispatcher()
        wf = create_workflow(client, [{"id": "bad", "type": "mock", "config": {}}])
        run_id = start_run(client, wf)["id"]
        assert crashing_worker("bad:before_execute:99").returncode == 137  # first owner crashes
        assert crashing_worker("bad:before_execute:99").returncode == 137  # takeover #1 crashes
        stack.start_worker(concurrency=1)  # takeover #2 exceeds the budget of 1
        run = wait_status(client, run_id, {"SUCCEEDED", "FAILED"}, timeout=30)
        assert run["status"] == "FAILED" and "recovery budget" in run["error"]
    finally:
        settings.max_run_recoveries = old
