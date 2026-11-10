"""API -> outbox -> Redis -> worker -> PostgreSQL, against real services."""

import concurrent.futures as cf
import time

import pytest
from sqlalchemy import func, select, text

from conftest import API, create_workflow, start_run, wait_status

pytestmark = pytest.mark.integration

TERMINAL = {"SUCCEEDED", "FAILED", "REJECTED", "CANCELLED"}


def mock(id, **cfg):
    retry = cfg.pop("retry", {"max_attempts": 3, "backoff_seconds": 0.05})
    timeout = cfg.pop("timeout_seconds", 30)
    return {"id": id, "type": "mock", "config": cfg, "retry": retry, "timeout_seconds": timeout}


def edge(a, b):
    return {"source": a, "target": b}


# ---------------------------------------------------------------- execution


def test_fan_out_fan_in_dag(client, stack):
    stack.start_dispatcher()
    stack.start_worker()
    wf = create_workflow(client, [
        {"id": "start", "type": "transform", "config": {"template": "doc={{input.doc}}"}},
        {"id": "left", "type": "llm", "config": {"prompt": "L {{nodes.start.text}}"}},
        mock("right", output="{{nodes.start.text}}"),
        {"id": "join", "type": "transform",
         "config": {"template": {"l": "{{nodes.left.text}}", "r": "{{nodes.right.value}}"}}},
    ], [edge("start", "left"), edge("start", "right"), edge("left", "join"), edge("right", "join")])
    run = wait_status(client, start_run(client, wf, {"doc": "D"})["id"], TERMINAL)
    assert run["status"] == "SUCCEEDED", run
    joined = run["output"]["result"]["join"]["value"]
    assert joined["r"] == "doc=D" and "L doc=D" in joined["l"]
    assert all(s["status"] == "SUCCEEDED" and s["attempts"] == 1 for s in run["steps"])


def test_retry_then_success(client, stack):
    stack.start_dispatcher()
    stack.start_worker()
    wf = create_workflow(client, [mock("flaky", output="ok", fail_times=2)])
    run = wait_status(client, start_run(client, wf)["id"], TERMINAL)
    assert run["status"] == "SUCCEEDED"
    step = run["steps"][0]
    assert step["attempts"] == 3 and [e["retryable"] for e in step["errors"]] == [True, True]


def test_retry_budget_exhausted(client, stack):
    stack.start_dispatcher()
    stack.start_worker()
    wf = create_workflow(client, [mock("flaky", fail_times=10, retry={"max_attempts": 2, "backoff_seconds": 0})])
    run = wait_status(client, start_run(client, wf)["id"], TERMINAL)
    assert run["status"] == "FAILED" and "flaky" in run["error"]
    assert run["steps"][0]["attempts"] == 2


def test_fatal_error_is_not_retried(client, stack):
    stack.start_dispatcher()
    stack.start_worker()
    wf = create_workflow(client, [mock("boom", fatal=True), mock("after")], [edge("boom", "after")])
    run = wait_status(client, start_run(client, wf)["id"], TERMINAL)
    assert run["status"] == "FAILED"
    assert run["steps"][0]["attempts"] == 1 and run["steps"][0]["errors"][0]["retryable"] is False
    assert run["steps"][1]["status"] == "NOT_STARTED"


def test_node_timeout(client, stack):
    stack.start_dispatcher()
    stack.start_worker()
    wf = create_workflow(client, [mock("slow", sleep_seconds=3, timeout_seconds=0.3,
                                       retry={"max_attempts": 1, "backoff_seconds": 0})])
    run = wait_status(client, start_run(client, wf)["id"], TERMINAL)
    assert run["status"] == "FAILED" and "timeout" in run["error"]


# ---------------------------------------------------------------- approvals


def approval_wf(client):
    return create_workflow(client, [
        mock("prepare", output="draft"),
        {"id": "gate", "type": "approval", "config": {"message": "ok?"}},
        mock("publish", output="{{nodes.prepare.value}}", side_effect=True),
    ], [edge("prepare", "gate"), edge("gate", "publish")])


def test_approval_approve_resumes(client, stack):
    stack.start_dispatcher()
    stack.start_worker()
    run_id = start_run(client, approval_wf(client))["id"]
    run = wait_status(client, run_id, {"WAITING_APPROVAL"})
    assert run["pending_approvals"] == [{"node_id": "gate", "message": "ok?"}]
    r = client.post(f"/v1/runs/{run_id}/approve", json={"decision": "approve"}, headers={"X-Actor-Id": "alice"})
    assert r.status_code == 200 and r.json()["actor"] == "alice"
    run = wait_status(client, run_id, TERMINAL)
    assert run["status"] == "SUCCEEDED"
    assert run["output"]["result"]["publish"]["value"] == "draft"
    gate = next(s for s in run["steps"] if s["node_id"] == "gate")
    assert gate["output"]["actor"] == "alice"


def test_approval_reject(client, stack):
    stack.start_dispatcher()
    stack.start_worker()
    run_id = start_run(client, approval_wf(client))["id"]
    wait_status(client, run_id, {"WAITING_APPROVAL"})
    client.post(f"/v1/runs/{run_id}/approve", json={"decision": "reject", "comment": "no"})
    run = wait_status(client, run_id, TERMINAL)
    assert run["status"] == "REJECTED" and "no" in run["error"]
    assert next(s for s in run["steps"] if s["node_id"] == "publish")["status"] == "NOT_STARTED"


def test_concurrent_approvals_have_one_winner(client, stack):
    stack.start_dispatcher()
    stack.start_worker()
    run_id = start_run(client, approval_wf(client))["id"]
    wait_status(client, run_id, {"WAITING_APPROVAL"})
    decisions = ["approve", "reject"] * 4
    with cf.ThreadPoolExecutor(8) as ex:
        codes = list(ex.map(lambda d: client.post(f"/v1/runs/{run_id}/approve",
                                                  json={"decision": d}).status_code, decisions))
    assert codes.count(200) == 1 and codes.count(409) == 7, codes
    run = wait_status(client, run_id, TERMINAL)
    assert run["status"] in {"SUCCEEDED", "REJECTED"}


# ---------------------------------------------------------------- cancel


def test_cancel_queued_run(client):
    # No dispatcher/worker: the run sits in PENDING.
    run_id = start_run(client, create_workflow(client, [mock("a")]))["id"]
    r = client.post(f"/v1/runs/{run_id}/cancel")
    assert r.status_code == 202 and r.json()["status"] == "CANCELLED"
    assert client.post(f"/v1/runs/{run_id}/cancel").status_code == 409


def test_cancel_running_run(client, stack):
    stack.start_dispatcher()
    stack.start_worker()
    wf = create_workflow(client, [mock("a", sleep_seconds=1), mock("b", sleep_seconds=1), mock("c")],
                         [edge("a", "b"), edge("b", "c")])
    run_id = start_run(client, wf)["id"]
    wait_status(client, run_id, {"RUNNING"})
    assert client.post(f"/v1/runs/{run_id}/cancel").status_code == 202
    run = wait_status(client, run_id, TERMINAL)
    assert run["status"] == "CANCELLED"
    assert run["steps"][2]["status"] == "NOT_STARTED"


# ---------------------------------------------------------------- API contract


def test_idempotent_start(client):
    wf = create_workflow(client, [mock("a")])
    a = start_run(client, wf, {"x": 1}, key="abc")
    b = client.post(f"/v1/workflows/{wf}/runs", json={"input": {"x": 1}}, headers={"Idempotency-Key": "abc"})
    assert b.status_code == 200 and b.json()["id"] == a["id"]
    c = client.post(f"/v1/workflows/{wf}/runs", json={"input": {"x": 2}}, headers={"Idempotency-Key": "abc"})
    assert c.status_code == 409


def test_versioning_and_validation(client):
    v1 = client.post("/v1/workflows", json={"name": "same", "nodes": [mock("a")]}).json()
    v2 = client.post("/v1/workflows", json={"name": "same", "nodes": [mock("a"), mock("b")]}).json()
    assert (v1["version"], v2["version"]) == (1, 2)
    bad = client.post("/v1/workflows", json={"name": "x", "nodes": [mock("a"), mock("b")],
                                             "edges": [edge("a", "b"), edge("b", "a")]})
    assert bad.status_code == 422 and "cycle" in str(bad.json())


def test_missing_input_rejected(client):
    wf = create_workflow(client, [{"id": "a", "type": "transform", "config": {"template": "{{input.doc}}"}}])
    r = client.post(f"/v1/workflows/{wf}/runs", json={"input": {}})
    assert r.status_code == 422 and "doc" in str(r.json())


def test_tenant_isolation_and_auth(client):
    wf = create_workflow(client, [mock("a")])
    run_id = start_run(client, wf)["id"]
    other = {"X-API-Key": "other-key"}
    assert client.get(f"/v1/runs/{run_id}", headers=other).status_code == 404
    assert client.get(f"/v1/workflows/{wf}", headers=other).status_code == 404
    assert client.post(f"/v1/runs/{run_id}/cancel", headers=other).status_code == 404
    assert client.get("/v1/runs", headers={"X-API-Key": "nope"}).status_code == 401


def test_tenant_quota(client):
    from invictus.config import get_settings

    settings = get_settings()
    old = settings.max_active_runs_per_tenant
    settings.max_active_runs_per_tenant = 2
    try:
        wf = create_workflow(client, [mock("a")])
        start_run(client, wf)
        start_run(client, wf)
        assert client.post(f"/v1/workflows/{wf}/runs", json={"input": {}}).status_code == 429
    finally:
        settings.max_active_runs_per_tenant = old


# ---------------------------------------------------------------- durability


def test_outbox_holds_runs_until_dispatcher_runs(client, stack):
    from invictus.db import session_scope

    run_id = start_run(client, create_workflow(client, [mock("a")]))["id"]
    with session_scope() as s:
        assert s.scalar(text("SELECT count(*) FROM outbox WHERE published_at IS NULL")) == 1
    time.sleep(0.5)
    assert client.get(f"/v1/runs/{run_id}").json()["status"] == "PENDING"
    stack.start_dispatcher()
    stack.start_worker()
    assert wait_status(client, run_id, TERMINAL)["status"] == "SUCCEEDED"


def test_sweeper_recovers_runs_lost_from_redis(client, stack):
    from invictus.config import get_settings
    from invictus.dispatcher import relay_once, sweep_once
    from invictus.queue import RunQueue, get_redis

    q = RunQueue()
    q.ensure_group()
    run_id = start_run(client, create_workflow(client, [mock("a")]))["id"]
    assert relay_once(q) == 1
    get_redis().delete(get_settings().stream_key)        # simulate Redis losing its data
    q.ensure_group()
    assert sweep_once(stale_seconds=0) == 1
    stack.start_dispatcher()
    stack.start_worker()
    assert wait_status(client, run_id, TERMINAL)["status"] == "SUCCEEDED"


def test_duplicate_delivery_executes_once(client, stack):
    from invictus.db import session_scope
    from invictus.models import ToolExecution
    from invictus.queue import RunQueue

    stack.start_dispatcher()
    wf = create_workflow(client, [mock("effect", side_effect=True, sleep_seconds=0.3)])
    run_id = start_run(client, wf)["id"]
    q = RunQueue()
    q.ensure_group()
    for _ in range(5):
        q.enqueue(run_id, "duplicate")
    stack.start_worker(concurrency=4)
    assert wait_status(client, run_id, TERMINAL)["status"] == "SUCCEEDED"
    time.sleep(0.5)
    with session_scope() as s:
        assert s.scalar(select(func.count()).select_from(ToolExecution)) == 1


def test_lease_fencing_blocks_zombie_writes(client):
    from invictus.db import session_scope
    from invictus.engine import RunContext, _finish_attempt, _start_attempt
    from invictus.errors import LeaseLost
    from invictus.worker import acquire_lease

    run_id = start_run(client, create_workflow(client, [mock("a")]))["id"]
    assert acquire_lease(run_id, "worker-A", 30).took_over_from is None
    zombie = RunContext(run_id=run_id, worker_id="worker-A", recovery_count=0)
    attempt_id, _ = _start_attempt(zombie, "a", 3)
    with session_scope() as s:  # A stalls past its lease
        s.execute(text("UPDATE workflow_runs SET lease_expires_at = now() - interval '1 second' WHERE id = :i"),
                  {"i": run_id})
    claim = acquire_lease(run_id, "worker-B", 30)
    assert claim.took_over_from == "worker-A" and claim.recovery_count == 1
    with pytest.raises(LeaseLost):
        from invictus.models import AttemptStatus
        _finish_attempt(zombie, attempt_id, AttemptStatus.SUCCEEDED, output={"stale": True})
