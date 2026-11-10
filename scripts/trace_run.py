"""Trace one run through Invictus and print the internal state at every stage.

    python scripts/trace_run.py

Uses the database/Redis from INVICTUS_DATABASE_URL / INVICTUS_REDIS_URL (or .env).
Drives the dispatcher step by step and runs workers as real subprocesses, killing the
first one mid-run, so you can see the outbox, queue, lease, attempts, checkpoints and
approval records change.
"""

import os
import subprocess
import sys
import time

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
os.environ.setdefault("INVICTUS_LEASE_SECONDS", "3")
os.environ["INVICTUS_LLM_PROVIDER"] = "fake"
os.environ["INVICTUS_FAULTS"] = ""

from fastapi.testclient import TestClient  # noqa: E402
from sqlalchemy import text  # noqa: E402

from invictus.api import app  # noqa: E402
from invictus.db import session_scope  # noqa: E402
from invictus.dispatcher import relay_once  # noqa: E402
from invictus.queue import RunQueue  # noqa: E402

WORKFLOW = {
    "name": "trace-demo",
    "nodes": [
        {"id": "extract", "type": "transform", "config": {"template": "Doc: {{input.document}}"}},
        {"id": "summarize", "type": "llm", "config": {"prompt": "Summarise {{nodes.extract.text}}"}},
        {"id": "charge", "type": "mock", "config": {"output": "charged", "side_effect": True, "sleep_seconds": 0.5}},
        {"id": "review", "type": "approval", "config": {"message": "Ship it?"}},
        {"id": "codegen", "type": "llm", "config": {"prompt": "Code for {{nodes.summarize.text}}"}},
    ],
    "edges": [
        {"source": "extract", "target": "summarize"}, {"source": "extract", "target": "charge"},
        {"source": "summarize", "target": "review"}, {"source": "charge", "target": "review"},
        {"source": "review", "target": "codegen"},
    ],
}


def banner(title: str) -> None:
    print(f"\n{'=' * 78}\n{title}\n{'=' * 78}")


def snapshot(run_id: str, q: RunQueue) -> None:
    with session_scope() as s:
        r = s.execute(text("""
            SELECT status, lease_owner, round(extract(epoch FROM lease_expires_at - now())::numeric, 1),
                   recovery_count, cancel_requested
            FROM workflow_runs WHERE id = :id"""), {"id": run_id}).first()
        outbox = s.execute(text("SELECT kind, published_at IS NOT NULL FROM outbox WHERE run_id = :id ORDER BY id"),
                           {"id": run_id}).all()
        attempts = s.execute(text("""
            SELECT node_id, attempt, status, coalesce(error, '') FROM node_attempts
            WHERE run_id = :id ORDER BY started_at, attempt"""), {"id": run_id}).all()
        approvals = s.execute(text("SELECT node_id, decision, actor_id FROM approval_events WHERE run_id = :id"),
                              {"id": run_id}).all()
        ckpts = s.execute(text("""
            SELECT metadata->>'step', metadata->>'source' FROM checkpoints
            WHERE thread_id = :id AND checkpoint_ns = '' ORDER BY checkpoint_id"""), {"id": run_id}).all()
        execs = s.scalar(text("SELECT count(*) FROM tool_executions WHERE idempotency_key LIKE :k"), {"k": run_id + ":%"})
        effects = s.scalar(text("SELECT count(*) FROM side_effects WHERE idempotency_key LIKE :k"), {"k": run_id + ":%"})
    owner = (r[1] or "-")
    print(f"  run           status={r[0]}  lease_owner={owner[-22:]}  lease_left={r[2]}s  "
          f"recovery_count={r[3]}  cancel={r[4]}")
    print(f"  outbox        {[(k, 'published' if p else 'UNPUBLISHED') for k, p in outbox]}")
    st = q.stats()
    print(f"  redis stream  length={st['stream_length']}  pending(unacked)={st['pending']}  dead_letters={st['dead_letters']}")
    print("  node_attempts " + ("(none)" if not attempts else ""))
    for node, n, status, err in attempts:
        print(f"     {node:<10} #{n} {status:<17} {err[:45]}")
    print(f"  approvals     {[tuple(a) for a in approvals] or '(none)'}")
    print(f"  checkpoints   {len(ckpts)} saved; steps={[int(x[0]) for x in ckpts]}")
    print(f"  side effects  physical executions={execs}  distinct effects={effects}")


def spawn_worker(fault: str = "") -> subprocess.Popen:
    env = {**os.environ, "INVICTUS_FAULTS": fault, "LOG_LEVEL": "WARNING"}
    return subprocess.Popen([sys.executable, "-m", "invictus.worker", "--concurrency", "1"], cwd=ROOT, env=env)


def wait_for(client, run_id, statuses, timeout=30) -> dict:
    end = time.time() + timeout
    while time.time() < end:
        run = client.get(f"/v1/runs/{run_id}").json()
        if run["status"] in statuses:
            return run
        relay_once(RunQueue())  # act as the dispatcher
        time.sleep(0.2)
    raise TimeoutError(run["status"])


def main() -> None:
    q = RunQueue()
    with TestClient(app) as client:
        client.headers.update({"X-API-Key": os.environ.get("TRACE_API_KEY", "dev-key")})
        q.ensure_group()
        relay_once(q)  # flush anything left over from earlier runs
        wf = client.post("/v1/workflows", json=WORKFLOW).json()

        banner("1. POST /v1/workflows/{id}/runs  -> run row + outbox row, one transaction")
        run_id = client.post(f"/v1/workflows/{wf['id']}/runs", json={"input": {"document": "CSV export"}},
                             headers={"Idempotency-Key": f"trace-{time.time()}"}).json()["id"]
        print(f"  run_id={run_id}  (workflow v{wf['version']})")
        snapshot(run_id, q)

        banner("2. Dispatcher relays the outbox to the Redis stream")
        relay_once(q)
        snapshot(run_id, q)

        banner("3. Worker A claims it, runs extract/summarize/charge, then is KILLED after the\n"
               "   'charge' side effect happened but before its result was saved")
        a = spawn_worker("charge:after_side_effect")
        print(f"  worker A exited with code {a.wait(timeout=60)} (137 = killed, like SIGKILL)")
        snapshot(run_id, q)

        banner("4. Lease expires -> Worker B reclaims the message, takes over, resumes from the\n"
               "   checkpoint, re-runs only 'charge', and parks at the approval gate")
        t0 = time.time()
        b = spawn_worker()
        run = wait_for(client, run_id, {"WAITING_APPROVAL", "FAILED"})
        print(f"  reached {run['status']} {time.time() - t0:.1f}s after worker B started")
        snapshot(run_id, q)

        banner("5. A human approves -> approval_events row + outbox 'resume' event")
        r = client.post(f"/v1/runs/{run_id}/approve", json={"decision": "approve", "comment": "LGTM"},
                        headers={"X-Actor-Id": "alice"})
        print(f"  POST /approve -> {r.status_code} {r.json()}")
        dup = client.post(f"/v1/runs/{run_id}/approve", json={"decision": "reject"})
        print(f"  second decision -> {dup.status_code} {dup.json()['detail']}")
        snapshot(run_id, q)

        banner("6. Worker B resumes from the interrupt checkpoint and finishes")
        run = wait_for(client, run_id, {"SUCCEEDED", "FAILED", "REJECTED", "CANCELLED"})
        snapshot(run_id, q)
        print(f"  result: {run['output']['result']}")

        banner("7. Audit log")
        with session_scope() as s:
            for actor, action, at in s.execute(text(
                    "SELECT actor_id, action, to_char(at, 'HH24:MI:SS.MS') FROM audit_log WHERE run_id = :id "
                    "ORDER BY id"), {"id": run_id}):
                print(f"  {at}  {action:<18} {actor[-30:]}")

        b.terminate()
        b.wait(timeout=30)


if __name__ == "__main__":
    main()
