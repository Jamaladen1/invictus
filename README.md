# Invictus — AI Workflow Orchestration

Versioned, multi-stage AI workflows (document analysis, code generation, human approval)
with asynchronous execution, retries, durable progress, and recovery after worker crashes.

**Stack:** Python · FastAPI · LangGraph · PostgreSQL · Redis Streams · Docker

```
Client ─▶ FastAPI ──(one txn: run + outbox row)──▶ PostgreSQL ◀── LangGraph checkpoints
                                                       │              node_attempts, approvals
                                         dispatcher ◀──┘ (outbox relay + sweeper)
                                              │ XADD
                                              ▼
                                     Redis Stream (consumer group)
                                              │ XREADGROUP / XAUTOCLAIM
                                              ▼
                     worker pool ─ PG lease ─▶ LangGraph graph ─▶ tools (llm, http, transform, mock)
Approval ─▶ FastAPI ─▶ approval_events + outbox(resume) ─▶ worker resumes from checkpoint
```

## Quick start

```bash
docker compose up -d --build          # api :8000, dispatcher, 2 workers, postgres, redis
python scripts/demo.py                 # create workflow, run, approve, print result
```

Use Claude for `llm` nodes: `INVICTUS_LLM_PROVIDER=anthropic ANTHROPIC_API_KEY=... docker compose up -d`.
The default `fake` provider is offline and deterministic. Interactive API docs: http://localhost:8000/docs

## API

All calls need `X-API-Key` (maps to a tenant via `INVICTUS_API_KEYS="key:tenant,..."`).
`X-Actor-Id` names the human for approvals and the audit log.

| Method | Path | Purpose |
|---|---|---|
| POST | `/v1/workflows` | Validate and store a definition; same `name` → next version |
| GET | `/v1/workflows[/{id}]` | List / fetch definitions |
| POST | `/v1/workflows/{id}/runs` | Start a run; optional `Idempotency-Key` header |
| GET | `/v1/runs[/{id}]` | Status, per-step attempts, outputs, errors, pending approvals |
| POST | `/v1/runs/{id}/approve` | `{"decision":"approve"\|"reject","node_id"?,"comment"?}` |
| POST | `/v1/runs/{id}/cancel` | Request cancellation |
| GET | `/v1/metrics` | Runs by status, recovery counts, p50/p95 latency, queue depth |
| GET | `/healthz`, `/readyz` | Liveness / readiness (Postgres + Redis) |

### Workflow definition

```json
{
  "name": "doc-review",
  "nodes": [
    {"id": "extract",   "type": "transform", "config": {"template": "Doc: {{input.document}}"}},
    {"id": "summarize", "type": "llm", "config": {"prompt": "Summarise {{nodes.extract.text}}"},
     "retry": {"max_attempts": 3, "backoff_seconds": 1}, "timeout_seconds": 120},
    {"id": "review",    "type": "approval", "config": {"message": "Approve?"}},
    {"id": "codegen",   "type": "llm", "config": {"prompt": "Implement: {{nodes.summarize.text}}"}}
  ],
  "edges": [{"source": "extract", "target": "summarize"},
            {"source": "summarize", "target": "review"},
            {"source": "review", "target": "codegen"}]
}
```

Validation rejects: duplicate ids, unknown tool types, bad tool config, unknown edge ends,
self-loops, cycles, and templates that read a node that is not upstream. Required inputs are
derived from `{{input.*}}` references and checked when a run starts.

Node types: `llm` (fake or Claude), `transform` (templating), `http` (allow-listed hosts only,
sends `Idempotency-Key`), `approval` (human gate), `mock` (test tool: sleeps, failures, side effects).

## How execution and recovery work

1. **Start** — the run row and an outbox row commit in one transaction. If Redis is down the
   run waits in the outbox; it is never silently lost.
2. **Dispatch** — the dispatcher relays outbox rows to a Redis Stream (`FOR UPDATE SKIP LOCKED`).
3. **Claim** — a worker reads the message and takes a **PostgreSQL lease** on the run. Redis
   decides who *might* work on a run; the lease decides who *does*. Duplicate deliveries find
   the lease held (or the run finished) and are acked without executing.
4. **Execute** — the definition compiles to a LangGraph `StateGraph` (fan-out/fan-in supported),
   checkpointed to Postgres after every step (`durability="sync"`, `thread_id = run id`).
   Each node attempt is recorded in `node_attempts`; failures are classified retryable vs fatal
   and retried with capped exponential backoff + full jitter.
5. **Heartbeat** — renews the DB lease and the Redis message's idle timer. All result writes
   are **fenced** on still owning the lease, so a stalled "zombie" worker cannot overwrite a newer owner.
6. **Crash** — the lease expires; another worker reclaims the message (`XAUTOCLAIM`), takes the
   lease (`recovery_count += 1`), marks the dead attempt `ABANDONED` (doesn't use up retries),
   and resumes from the last LangGraph checkpoint. A node whose result was already persisted
   returns it without re-running.
7. **Approval** — the node calls LangGraph `interrupt()`; the run parks as `WAITING_APPROVAL`
   with no lease held. A decision is a row in `approval_events` (unique per gate, so concurrent
   decisions have exactly one winner) plus an outbox `resume` event.

### Crash between side effect and checkpoint

| Worker dies at | Tool executions | Visible effects | Why |
|---|---|---|---|
| before the tool runs | 1 | 1 | attempt abandoned, rerun once |
| after the tool, before persisting | 2 | 1 | re-executed; the stable key `run_id:node_id` dedupes downstream |
| after persisting, before checkpoint | 1 | 1 | the persisted result is reused |

Execution is at-least-once. Exactly-once *effects* need a downstream that honours the idempotency key.

### Safeguards

- **Poison runs**: more than `INVICTUS_MAX_RUN_RECOVERIES` lost workers → `FAILED`.
- **Dead letters**: more than `INVICTUS_MAX_DELIVERIES` deliveries → DLQ stream, run `FAILED`.
- **Sweeper**: re-enqueues runs nobody owns (e.g. Redis lost its data, resume message lost).
- **Quotas**: max active runs per tenant (`429`). **Tenancy**: every query is tenant-scoped.
- **Secrets**: API keys, bearer tokens, `key=value` secrets are redacted from stored errors.
- **Audit log**: workflow creation, run start, approvals, cancels, worker takeovers (`run.recovered`, with the
  previous owner and abandoned attempts), terminal states.
- **Graceful shutdown**: SIGTERM stops new claims and lets in-flight runs finish.

## Tests

```bash
python -m venv .venv && .venv/bin/pip install -e '.[dev]'
docker run -d --name inv-pg -e POSTGRES_USER=invictus -e POSTGRES_PASSWORD=invictus -p 5433:5432 postgres:16-alpine
docker run -d --name inv-redis -p 6380:6379 redis:7-alpine
docker exec inv-pg psql -U invictus -c "CREATE DATABASE invictus_test"
.venv/bin/pytest
```

- `tests/test_unit.py` — DAG validation, templating, retry classification, backoff, redaction (no services).
- `tests/test_integration.py` — API → outbox → Redis → worker → Postgres: fan-in, retries, fatal errors,
  timeouts, approvals (incl. 8 concurrent decisions → 1 winner), cancel, idempotency, versioning,
  tenant isolation, quotas, outbox durability, sweeper after Redis data loss, duplicate delivery, lease fencing.
- `tests/test_recovery.py` — kills a real worker process at each crash boundary above, and checks poison-run handling.
- `tests/test_edge_cases.py` — races and graph shapes: approval or cancel landing while a worker still owns the run,
  parallel approval gates, uneven diamonds, 40-way fan-out, quota under concurrent starts, SIGTERM drain.

Integration tests skip automatically if Postgres/Redis are unreachable.

## Measuring recovery

```bash
python scripts/recovery_experiment.py --runs 100 --workers 3 --kill-every 1.5
```

Submits runs of a 5-node workflow, SIGKILLs a random worker every ~1.5 s, and reports:
interrupted runs (`recovery_count > 0`), recovered (`SUCCEEDED`), recovery rate, unrecovered
reasons, physical vs distinct side effects, and latency of normal vs recovered runs.
Results on a laptop (3 worker processes × 2 threads, lease 3 s, fake LLM, kill interval randomised ±50%):

| Runs | Seed | Kill every | Kills | Interrupted | Recovered | Rate | Duplicate effects |
|---|---|---|---|---|---|---|---|
| 120 | 7 | ~1.0 s | 28 | 35 | 35 | 100% | 0 |
| 300 | 11 | ~1.0 s | 60 | 70 | 70 | 100% | 0 |
| 300 | 23 | ~0.5 s | 104 | 76 | 73 | 96.1% | 0 |
| **600 (300+300)** | | | **164** | **146** | **143** | **97.9%** | **0** |

The 3 unrecovered runs each lost 6 different workers (all 6 takeovers are in the audit log) and were
deliberately failed by the poison-run safeguard (`INVICTUS_MAX_RUN_RECOVERIES=5`). Raising the budget
trades a higher recovery rate for longer crash loops on a genuinely poisonous run. Random kills rarely
land in the narrow after-side-effect window, so that case is covered deterministically by
`tests/test_recovery.py`. Latency figures from these runs are dominated by queue backlog (submissions
outpace 6 worker slots) and are not meaningful.
Quote the procedure, sample size and denominator with any figure.

## Tradeoffs and open decisions

- Redis is the dispatch layer, not the system of record; Postgres is. Losing Redis costs latency, not runs.
- Synchronous checkpoints after every step cost write amplification for durability.
- A timed-out tool thread can't be killed in Python; its result is ignored. Use tool-native timeouts.
- Schema is created with `create_all`; production would use migrations (Alembic).
- One API key = one tenant; real deployments need proper auth (OIDC) and per-approver authorization.
