"""Adversarial tests: races and graph shapes the basic suite doesn't cover."""

import concurrent.futures as cf
import os
import signal
import subprocess
import sys
import time

import pytest

from conftest import create_workflow, start_run, wait_status

pytestmark = pytest.mark.integration
TERMINAL = {"SUCCEEDED", "FAILED", "REJECTED", "CANCELLED"}
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def mock(id, **cfg):
    return {"id": id, "type": "mock", "config": cfg, "retry": {"max_attempts": 3, "backoff_seconds": 0.05}}


def gate(id):
    return {"id": id, "type": "approval", "config": {"message": id}}


def edge(a, b):
    return {"source": a, "target": b}


def wait_gate_pending(client, run_id, node_id, timeout=15):
    deadline = time.time() + timeout
    while time.time() < deadline:
        run = client.get(f"/v1/runs/{run_id}").json()
        if any(p["node_id"] == node_id for p in run["pending_approvals"]):
            return run
        time.sleep(0.05)
    raise AssertionError(f"gate {node_id} never became pending: {run}")


def test_approval_decided_while_worker_still_owns_run(client, stack):
    """Gate pauses while a sibling is still running. Approving in that window must not be lost."""
    stack.start_dispatcher()
    stack.start_worker(concurrency=2)
    wf = create_workflow(client, [gate("gate"), mock("slow", sleep_seconds=2), mock("after")],
                         [edge("gate", "after"), edge("slow", "after")])
    run_id = start_run(client, wf)["id"]
    run = wait_gate_pending(client, run_id, "gate")
    assert run["status"] == "RUNNING"  # the sibling is still executing
    assert client.post(f"/v1/runs/{run_id}/approve", json={"decision": "approve"}).status_code == 200
    run = wait_status(client, run_id, TERMINAL, timeout=15)
    assert run["status"] == "SUCCEEDED", run


def test_cancel_while_run_is_pausing_for_approval(client, stack):
    stack.start_dispatcher()
    stack.start_worker(concurrency=2)
    wf = create_workflow(client, [gate("gate"), mock("slow", sleep_seconds=2), mock("after")],
                         [edge("gate", "after"), edge("slow", "after")])
    run_id = start_run(client, wf)["id"]
    wait_gate_pending(client, run_id, "gate")
    assert client.post(f"/v1/runs/{run_id}/cancel").status_code == 202
    run = wait_status(client, run_id, TERMINAL, timeout=15)
    assert run["status"] == "CANCELLED", run


def test_parallel_approval_gates(client, stack):
    stack.start_dispatcher()
    stack.start_worker()
    wf = create_workflow(client, [mock("start"), gate("legal"), gate("security"), mock("ship")],
                         [edge("start", "legal"), edge("start", "security"),
                          edge("legal", "ship"), edge("security", "ship")])
    run_id = start_run(client, wf)["id"]
    run = wait_status(client, run_id, {"WAITING_APPROVAL"})
    deadline = time.time() + 10
    while len(run["pending_approvals"]) < 2 and time.time() < deadline:
        time.sleep(0.1)
        run = client.get(f"/v1/runs/{run_id}").json()
    assert {p["node_id"] for p in run["pending_approvals"]} == {"legal", "security"}
    assert client.post(f"/v1/runs/{run_id}/approve", json={"decision": "approve"}).status_code == 422
    assert client.post(f"/v1/runs/{run_id}/approve", json={"decision": "approve", "node_id": "legal"}).status_code == 200
    run = wait_status(client, run_id, {"WAITING_APPROVAL"})
    time.sleep(1)
    run = client.get(f"/v1/runs/{run_id}").json()
    assert run["status"] == "WAITING_APPROVAL"
    assert [p["node_id"] for p in run["pending_approvals"]] == ["security"]
    client.post(f"/v1/runs/{run_id}/approve", json={"decision": "approve", "node_id": "security"})
    run = wait_status(client, run_id, TERMINAL)
    assert run["status"] == "SUCCEEDED"
    assert next(s for s in run["steps"] if s["node_id"] == "ship")["attempts"] == 1


def test_uneven_diamond_runs_join_once(client, stack):
    """a->b->c->d and a->d: d must wait for the long branch and run exactly once."""
    stack.start_dispatcher()
    stack.start_worker()
    wf = create_workflow(client, [
        mock("a", output="A"), mock("b", output="B"), mock("c", output="C"),
        {"id": "d", "type": "transform", "config": {"template": "{{nodes.a.value}}{{nodes.c.value}}"}},
    ], [edge("a", "b"), edge("b", "c"), edge("c", "d"), edge("a", "d")])
    run = wait_status(client, start_run(client, wf)["id"], TERMINAL)
    assert run["status"] == "SUCCEEDED", run
    assert run["output"]["result"]["d"]["text"] == "AC"
    assert all(s["attempts"] == 1 for s in run["steps"])


def test_wide_fan_out(client, stack):
    stack.start_dispatcher()
    stack.start_worker()
    leaves = [mock(f"n{i}", output=i) for i in range(40)]
    wf = create_workflow(client, [mock("root"), *leaves, mock("sink")],
                         [edge("root", f"n{i}") for i in range(40)] + [edge(f"n{i}", "sink") for i in range(40)])
    run = wait_status(client, start_run(client, wf)["id"], TERMINAL, timeout=60)
    assert run["status"] == "SUCCEEDED"
    assert len(run["output"]["nodes"]) == 42


def test_quota_holds_under_concurrent_starts(client):
    from invictus.config import get_settings

    settings = get_settings()
    old = settings.max_active_runs_per_tenant
    settings.max_active_runs_per_tenant = 3
    try:
        wf = create_workflow(client, [mock("a")])
        with cf.ThreadPoolExecutor(12) as ex:
            codes = list(ex.map(lambda _: client.post(f"/v1/workflows/{wf}/runs", json={"input": {}}).status_code,
                                range(12)))
        assert codes.count(201) == 3, codes
    finally:
        settings.max_active_runs_per_tenant = old


def test_sigterm_finishes_in_flight_run(client, stack):
    stack.start_dispatcher()
    wf = create_workflow(client, [mock("long", sleep_seconds=3, side_effect=True)])
    run_id = start_run(client, wf)["id"]
    proc = subprocess.Popen([sys.executable, "-m", "invictus.worker", "--concurrency", "1"], cwd=ROOT,
                            env={**os.environ, "LOG_LEVEL": "WARNING"})
    wait_status(client, run_id, {"RUNNING"})
    time.sleep(0.5)
    proc.send_signal(signal.SIGTERM)
    assert proc.wait(timeout=30) == 0
    run = client.get(f"/v1/runs/{run_id}").json()
    assert run["status"] == "SUCCEEDED" and run["recovery_count"] == 0, run
