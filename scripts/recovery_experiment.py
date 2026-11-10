"""Chaos experiment: measure workflow recovery under random worker kills.

    python scripts/recovery_experiment.py --runs 100 --workers 3 --kill-every 2.0

Procedure (write this down when quoting a recovery figure):
  1. Start a dispatcher and N worker processes against the configured PostgreSQL/Redis.
  2. Submit R runs of a 5-node workflow (fan-out/fan-in, one side-effecting node, sleeps so
     kills land mid-run).
  3. Every K seconds SIGKILL a random worker (no cleanup, no lease release, no ack) and
     start a replacement.
  4. Stop killing after the submission window, wait for every run to reach a terminal state
     (or the deadline).

Definitions:
  interrupted   run whose lease was taken over at least once (recovery_count > 0)
  recovered     interrupted run that reached SUCCEEDED
  recovery rate recovered / interrupted
  duplicates    physical side-effect executions minus distinct effects (at-least-once cost)
  visible dups  distinct effects beyond one per run (must be 0)
"""

import argparse
import os
import random
import signal
import statistics
import subprocess
import sys
import time
import uuid

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

WORKFLOW = {
    "nodes": [
        {"id": "ingest", "type": "mock", "config": {"output": "doc", "sleep_seconds": 0.3}},
        {"id": "analyze", "type": "mock", "config": {"output": "a", "sleep_seconds": 0.6}},
        {"id": "summarize", "type": "llm", "config": {"prompt": "sum {{nodes.ingest.value}}"}},
        {"id": "commit", "type": "mock", "config": {"output": "c", "sleep_seconds": 0.4, "side_effect": True}},
        {"id": "report", "type": "mock", "config": {"output": "{{nodes.commit.value}}", "sleep_seconds": 0.2}},
    ],
    "edges": [
        {"source": "ingest", "target": "analyze"}, {"source": "ingest", "target": "summarize"},
        {"source": "analyze", "target": "commit"}, {"source": "summarize", "target": "commit"},
        {"source": "commit", "target": "report"},
    ],
}


def spawn(module: str, env: dict, extra: list[str] = ()) -> subprocess.Popen:
    return subprocess.Popen([sys.executable, "-m", module, *extra], cwd=ROOT, env=env,
                            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def pct(values: list[float], q: float) -> float:
    if not values:
        return float("nan")
    values = sorted(values)
    return values[min(len(values) - 1, int(q * len(values)))]


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--runs", type=int, default=60)
    p.add_argument("--workers", type=int, default=3)
    p.add_argument("--concurrency", type=int, default=2)
    p.add_argument("--kill-every", type=float, default=2.0)
    p.add_argument("--lease", type=float, default=3.0)
    p.add_argument("--submit-over", type=float, default=20.0, help="seconds to spread submissions over")
    p.add_argument("--deadline", type=float, default=240.0)
    p.add_argument("--seed", type=int, default=7)
    args = p.parse_args()
    random.seed(args.seed)

    env = {**os.environ, "INVICTUS_LEASE_SECONDS": str(args.lease), "INVICTUS_LLM_PROVIDER": "fake",
           "INVICTUS_ENABLE_MOCK_TOOL": "true", "INVICTUS_FAULTS": "", "INVICTUS_SWEEP_STALE_SECONDS": "10",
           "INVICTUS_SWEEP_INTERVAL_SECONDS": "5", "INVICTUS_POLL_BLOCK_MS": "500", "LOG_LEVEL": "WARNING"}
    os.environ.update(env)

    from sqlalchemy import text

    from invictus.db import init_db, session_scope
    from invictus.models import OutboxEvent, WorkflowDefinition, WorkflowRun

    init_db()
    tenant = f"chaos-{uuid.uuid4().hex[:8]}"
    with session_scope() as s:
        d = WorkflowDefinition(tenant_id=tenant, name="chaos", version=1, graph_json={"name": "chaos", **WORKFLOW})
        s.add(d)
        s.flush()
        def_id = d.id

    dispatcher = spawn("invictus.dispatcher", env)
    workers = [spawn("invictus.worker", env, ["--concurrency", str(args.concurrency)]) for _ in range(args.workers)]
    kills = 0
    run_ids: list[str] = []
    start = time.time()
    next_kill = start + args.kill_every
    interval = args.submit_over / max(1, args.runs)
    try:
        while len(run_ids) < args.runs or time.time() < start + args.submit_over:
            if len(run_ids) < args.runs:
                with session_scope() as s:
                    run = WorkflowRun(tenant_id=tenant, definition_id=def_id, input_json={}, status="PENDING")
                    s.add(run)
                    s.flush()
                    s.add(OutboxEvent(run_id=run.id, kind="start"))
                    run_ids.append(run.id)
            if time.time() >= next_kill:
                victim = random.randrange(len(workers))
                workers[victim].send_signal(signal.SIGKILL)
                workers[victim].wait()
                workers[victim] = spawn("invictus.worker", env, ["--concurrency", str(args.concurrency)])
                kills += 1
                next_kill = time.time() + args.kill_every * random.uniform(0.5, 1.5)
            time.sleep(interval)

        print(f"submitted {len(run_ids)} runs, killed {kills} workers; waiting for completion...")
        deadline = time.time() + args.deadline
        while time.time() < deadline:
            with session_scope() as s:
                active = s.scalar(text("SELECT count(*) FROM workflow_runs WHERE tenant_id = :t AND status IN "
                                       "('PENDING','RUNNING','WAITING_APPROVAL')"), {"t": tenant})
            if active == 0:
                break
            time.sleep(1)
    finally:
        for w in workers:
            w.terminate()
        dispatcher.terminate()
        for proc in [*workers, dispatcher]:
            try:
                proc.wait(timeout=15)
            except subprocess.TimeoutExpired:
                proc.kill()

    with session_scope() as s:
        rows = s.execute(text("""
            SELECT status, recovery_count, error, extract(epoch FROM finished_at - created_at)
            FROM workflow_runs WHERE tenant_id = :t"""), {"t": tenant}).all()
        execs = s.scalar(text("SELECT count(*) FROM tool_executions te JOIN workflow_runs r "
                              "ON te.idempotency_key LIKE r.id || ':%' WHERE r.tenant_id = :t"), {"t": tenant})
        effects = s.scalar(text("SELECT count(*) FROM side_effects se JOIN workflow_runs r "
                                "ON se.idempotency_key LIKE r.id || ':%' WHERE r.tenant_id = :t"), {"t": tenant})

    by_status: dict[str, int] = {}
    for r in rows:
        by_status[r[0]] = by_status.get(r[0], 0) + 1
    interrupted = [r for r in rows if r[1] > 0]
    recovered = [r for r in interrupted if r[0] == "SUCCEEDED"]
    unrecovered = [r for r in interrupted if r[0] != "SUCCEEDED"]
    lat = [r[3] for r in rows if r[0] == "SUCCEEDED" and r[3] is not None]
    lat_int = [r[3] for r in recovered if r[3] is not None]
    succeeded = by_status.get("SUCCEEDED", 0)

    print("\n=== Invictus recovery experiment ===")
    print(f"runs={len(rows)} workers={args.workers}x{args.concurrency} kills={kills} lease={args.lease}s seed={args.seed}")
    print(f"final status: {by_status}")
    print(f"interrupted runs:      {len(interrupted)}")
    print(f"recovered (SUCCEEDED): {len(recovered)}")
    rate = 100 * len(recovered) / len(interrupted) if interrupted else float("nan")
    print(f"recovery rate:         {rate:.1f}%")
    if unrecovered:
        print("unrecovered reasons:   " + "; ".join(sorted({(r[2] or r[0])[:80] for r in unrecovered})))
    print(f"side effects: {execs} physical executions, {effects} distinct effects, "
          f"{succeeded} expected -> visible duplicates={max(0, effects - succeeded)}, "
          f"re-executions absorbed by idempotency={execs - effects}")
    print(f"latency (all succeeded):         p50={pct(lat, .5):.2f}s p95={pct(lat, .95):.2f}s")
    print(f"latency (interrupted+recovered): p50={pct(lat_int, .5):.2f}s p95={pct(lat_int, .95):.2f}s")
    if lat and lat_int:
        print(f"median recovery overhead:        {statistics.median(lat_int) - statistics.median(lat):.2f}s")


if __name__ == "__main__":
    main()
