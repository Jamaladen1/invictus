"""FastAPI control plane.

    uvicorn invictus.api:app
"""

import logging
import os
from contextlib import asynccontextmanager
from typing import Any, Literal

from fastapi import Depends, FastAPI, Header, HTTPException, Query, Response, status
from pydantic import BaseModel, Field
from sqlalchemy import func, select, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from .config import get_settings
from .db import get_engine, init_db, session_scope
from .models import (
    ACTIVE_STATUSES,
    TERMINAL_STATUSES,
    ApprovalEvent,
    AttemptStatus,
    AuditLog,
    NodeAttempt,
    OutboxEvent,
    RunStatus,
    WorkflowDefinition,
    WorkflowRun,
)
from .queue import RunQueue
from .spec import SpecError, WorkflowSpec, validate

log = logging.getLogger("invictus.api")


@asynccontextmanager
async def lifespan(_: FastAPI):
    if os.environ.get("INVICTUS_SKIP_INIT_DB") != "1":
        init_db()
    yield


app = FastAPI(title="Invictus", version="0.1.0", lifespan=lifespan,
              description="AI workflow orchestration: versioned DAGs, durable execution, approvals, recovery.")


# ---------------------------------------------------------------- auth


class Caller(BaseModel):
    tenant_id: str
    actor_id: str


def get_caller(x_api_key: str | None = Header(default=None),
               x_actor_id: str | None = Header(default=None)) -> Caller:
    tenant = get_settings().api_key_map().get(x_api_key or "")
    if tenant is None:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "missing or invalid X-API-Key")
    return Caller(tenant_id=tenant, actor_id=(x_actor_id or f"apikey:{tenant}")[:200])


def _audit(s: Session, caller: Caller, action: str, run_id: str | None = None, **detail: Any) -> None:
    s.add(AuditLog(tenant_id=caller.tenant_id, actor_id=caller.actor_id, action=action,
                   run_id=run_id, detail=detail))


# ---------------------------------------------------------------- schemas


class WorkflowOut(BaseModel):
    id: str
    name: str
    version: int
    graph: dict
    created_at: Any


class StartRunIn(BaseModel):
    input: dict[str, Any] = Field(default_factory=dict)


class ApproveIn(BaseModel):
    decision: Literal["approve", "reject"]
    node_id: str | None = None
    comment: str | None = Field(default=None, max_length=2000)


def _workflow_out(d: WorkflowDefinition) -> WorkflowOut:
    return WorkflowOut(id=d.id, name=d.name, version=d.version, graph=d.graph_json, created_at=d.created_at)


def _get_run(s: Session, caller: Caller, run_id: str, lock: bool = False) -> WorkflowRun:
    q = select(WorkflowRun).where(WorkflowRun.id == run_id, WorkflowRun.tenant_id == caller.tenant_id)
    if lock:
        q = q.with_for_update()
    run = s.scalar(q)
    if run is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "run not found")
    return run


def _run_view(s: Session, run: WorkflowRun) -> dict:
    definition = s.get(WorkflowDefinition, run.definition_id)
    spec = WorkflowSpec.model_validate(definition.graph_json)
    attempts = s.scalars(select(NodeAttempt).where(NodeAttempt.run_id == run.id)
                         .order_by(NodeAttempt.node_id, NodeAttempt.attempt)).all()
    by_node: dict[str, list[NodeAttempt]] = {}
    for a in attempts:
        by_node.setdefault(a.node_id, []).append(a)

    steps, pending = [], []
    for n in spec.nodes:
        rows = by_node.get(n.id, [])
        last = rows[-1] if rows else None
        steps.append({
            "node_id": n.id,
            "type": n.type,
            "status": last.status if last else "NOT_STARTED",
            "attempts": len([r for r in rows if r.status != AttemptStatus.ABANDONED.value]),
            "abandoned_attempts": len([r for r in rows if r.status == AttemptStatus.ABANDONED.value]),
            "output": next((r.output_json for r in rows if r.status == AttemptStatus.SUCCEEDED.value), None),
            "errors": [{"attempt": r.attempt, "error": r.error, "retryable": r.retryable}
                       for r in rows if r.error],
        })
        if last and last.status == AttemptStatus.WAITING_APPROVAL.value:
            decided = s.scalar(select(ApprovalEvent.id).where(ApprovalEvent.run_id == run.id,
                                                              ApprovalEvent.node_id == n.id))
            if decided is None:
                pending.append({"node_id": n.id, "message": n.config.get("message", "Approval required")})

    return {
        "id": run.id,
        "workflow_id": run.definition_id,
        "workflow_name": definition.name,
        "workflow_version": definition.version,
        "status": run.status,
        "input": run.input_json,
        "output": run.output_json,
        "error": run.error,
        "cancel_requested": run.cancel_requested,
        "recovery_count": run.recovery_count,
        "pending_approvals": pending,
        "steps": steps,
        "created_at": run.created_at,
        "started_at": run.started_at,
        "finished_at": run.finished_at,
    }


# ---------------------------------------------------------------- workflows


@app.post("/v1/workflows", status_code=201, response_model=WorkflowOut)
def create_workflow(body: dict, caller: Caller = Depends(get_caller)) -> WorkflowOut:
    try:
        spec = validate(body)
    except SpecError as e:
        raise HTTPException(422, {"errors": e.errors}) from e
    for _ in range(3):  # retry on a concurrent version bump
        try:
            with session_scope() as s:
                version = (s.scalar(select(func.max(WorkflowDefinition.version))
                                    .where(WorkflowDefinition.tenant_id == caller.tenant_id,
                                           WorkflowDefinition.name == spec.name)) or 0) + 1
                d = WorkflowDefinition(tenant_id=caller.tenant_id, name=spec.name, version=version,
                                       graph_json=spec.model_dump(mode="json"))
                s.add(d)
                s.flush()
                _audit(s, caller, "workflow.create", workflow_id=d.id, name=spec.name, version=version)
                return _workflow_out(d)
        except IntegrityError:
            continue
    raise HTTPException(status.HTTP_409_CONFLICT, "concurrent version conflict; retry")


@app.get("/v1/workflows", response_model=list[WorkflowOut])
def list_workflows(name: str | None = None, caller: Caller = Depends(get_caller)) -> list[WorkflowOut]:
    with session_scope() as s:
        q = select(WorkflowDefinition).where(WorkflowDefinition.tenant_id == caller.tenant_id)
        if name:
            q = q.where(WorkflowDefinition.name == name)
        q = q.order_by(WorkflowDefinition.name, WorkflowDefinition.version.desc()).limit(200)
        return [_workflow_out(d) for d in s.scalars(q)]


@app.get("/v1/workflows/{workflow_id}", response_model=WorkflowOut)
def get_workflow(workflow_id: str, caller: Caller = Depends(get_caller)) -> WorkflowOut:
    with session_scope() as s:
        d = s.scalar(select(WorkflowDefinition).where(WorkflowDefinition.id == workflow_id,
                                                      WorkflowDefinition.tenant_id == caller.tenant_id))
        if d is None:
            raise HTTPException(status.HTTP_404_NOT_FOUND, "workflow not found")
        return _workflow_out(d)


# ---------------------------------------------------------------- runs


@app.post("/v1/workflows/{workflow_id}/runs", status_code=201)
def start_run(workflow_id: str, body: StartRunIn, response: Response,
              idempotency_key: str | None = Header(default=None, max_length=200),
              caller: Caller = Depends(get_caller)) -> dict:
    def existing(s: Session) -> dict | None:
        if not idempotency_key:
            return None
        run = s.scalar(select(WorkflowRun).where(WorkflowRun.tenant_id == caller.tenant_id,
                                                 WorkflowRun.idempotency_key == idempotency_key))
        if run is None:
            return None
        if run.definition_id != workflow_id or run.input_json != body.input:
            raise HTTPException(status.HTTP_409_CONFLICT, "Idempotency-Key reused with a different request")
        response.status_code = status.HTTP_200_OK
        return _run_view(s, run)

    try:
        with session_scope() as s:
            d = s.scalar(select(WorkflowDefinition).where(WorkflowDefinition.id == workflow_id,
                                                          WorkflowDefinition.tenant_id == caller.tenant_id))
            if d is None:
                raise HTTPException(status.HTTP_404_NOT_FOUND, "workflow not found")
            if (prior := existing(s)) is not None:
                return prior
            missing = WorkflowSpec.model_validate(d.graph_json).required_inputs() - body.input.keys()
            if missing:
                raise HTTPException(422,
                                    {"errors": [f"missing input '{k}'" for k in sorted(missing)]})
            # Serialise starts per tenant so concurrent requests can't all pass the quota check.
            s.execute(text("SELECT pg_advisory_xact_lock(hashtext(:k))"), {"k": f"quota:{caller.tenant_id}"})
            active = s.scalar(select(func.count()).select_from(WorkflowRun)
                              .where(WorkflowRun.tenant_id == caller.tenant_id,
                                     WorkflowRun.status.in_([st.value for st in ACTIVE_STATUSES])))
            if active >= get_settings().max_active_runs_per_tenant:
                raise HTTPException(status.HTTP_429_TOO_MANY_REQUESTS, "tenant active-run quota exceeded")

            # Run + outbox row in one transaction: committed runs always get dispatched.
            run = WorkflowRun(tenant_id=caller.tenant_id, definition_id=d.id, input_json=body.input,
                              idempotency_key=idempotency_key, status=RunStatus.PENDING.value)
            s.add(run)
            s.flush()
            s.add(OutboxEvent(run_id=run.id, kind="start"))
            _audit(s, caller, "run.start", run.id, workflow_id=d.id)
            return _run_view(s, run)
    except IntegrityError:
        # Lost a race on the same Idempotency-Key: return the winner.
        with session_scope() as s:
            if (prior := existing(s)) is not None:
                return prior
        raise


@app.get("/v1/runs")
def list_runs(status_filter: RunStatus | None = Query(default=None, alias="status"),
              limit: int = Query(default=50, le=500), caller: Caller = Depends(get_caller)) -> list[dict]:
    with session_scope() as s:
        q = select(WorkflowRun).where(WorkflowRun.tenant_id == caller.tenant_id)
        if status_filter:
            q = q.where(WorkflowRun.status == status_filter.value)
        q = q.order_by(WorkflowRun.created_at.desc()).limit(limit)
        return [{"id": r.id, "workflow_id": r.definition_id, "status": r.status, "error": r.error,
                 "recovery_count": r.recovery_count, "created_at": r.created_at, "finished_at": r.finished_at}
                for r in s.scalars(q)]


@app.get("/v1/runs/{run_id}")
def get_run(run_id: str, caller: Caller = Depends(get_caller)) -> dict:
    with session_scope() as s:
        return _run_view(s, _get_run(s, caller, run_id))


@app.post("/v1/runs/{run_id}/approve")
def approve(run_id: str, body: ApproveIn, caller: Caller = Depends(get_caller)) -> dict:
    with session_scope() as s:
        run = _get_run(s, caller, run_id, lock=True)  # serialises concurrent decisions
        if run.status in {st.value for st in TERMINAL_STATUSES}:
            raise HTTPException(status.HTTP_409_CONFLICT, f"run is {run.status}")
        decided = select(ApprovalEvent.node_id).where(ApprovalEvent.run_id == run_id)
        q = select(NodeAttempt.node_id).where(NodeAttempt.run_id == run_id,
                                              NodeAttempt.status == AttemptStatus.WAITING_APPROVAL.value,
                                              NodeAttempt.node_id.not_in(decided))
        gates = list(s.scalars(q))
        if body.node_id:
            if body.node_id not in gates:
                raise HTTPException(status.HTTP_409_CONFLICT, f"no pending approval for node '{body.node_id}'")
            node_id = body.node_id
        elif len(gates) == 1:
            node_id = gates[0]
        elif not gates:
            already = s.scalar(select(func.count()).select_from(ApprovalEvent)
                               .where(ApprovalEvent.run_id == run_id))
            raise HTTPException(status.HTTP_409_CONFLICT,
                                "approval already decided" if already else "run has no pending approval")
        else:
            raise HTTPException(422,
                                {"errors": [f"several approvals pending; pass node_id (one of {gates})"]})
        try:
            with s.begin_nested():
                s.add(ApprovalEvent(run_id=run_id, node_id=node_id, actor_id=caller.actor_id,
                                    decision=body.decision, comment=body.comment))
        except IntegrityError as e:
            raise HTTPException(status.HTTP_409_CONFLICT, "approval already decided") from e
        s.add(OutboxEvent(run_id=run_id, kind="resume"))
        _audit(s, caller, f"approval.{body.decision}", run_id, node_id=node_id)
        return {"run_id": run_id, "node_id": node_id, "decision": body.decision, "actor": caller.actor_id}


@app.post("/v1/runs/{run_id}/cancel", status_code=202)
def cancel(run_id: str, caller: Caller = Depends(get_caller)) -> dict:
    with session_scope() as s:
        run = _get_run(s, caller, run_id, lock=True)
        if run.status in {st.value for st in TERMINAL_STATUSES}:
            raise HTTPException(status.HTTP_409_CONFLICT, f"run is already {run.status}")
        run.cancel_requested = True
        if run.lease_owner is None:
            # Nobody is executing it: cancel immediately.
            run.status = RunStatus.CANCELLED.value
            run.error = "cancelled"
            run.finished_at = func.now()
        _audit(s, caller, "run.cancel", run_id)
        s.flush()
        return {"run_id": run_id, "status": run.status, "cancel_requested": True}


# ---------------------------------------------------------------- ops


@app.get("/healthz")
def healthz() -> dict:
    return {"status": "ok"}


@app.get("/readyz")
def readyz(response: Response) -> dict:
    checks = {}
    try:
        with get_engine().connect() as c:
            c.execute(text("SELECT 1"))
        checks["postgres"] = "ok"
    except Exception as e:  # noqa: BLE001
        checks["postgres"] = f"error: {type(e).__name__}"
    try:
        RunQueue().r.ping()
        checks["redis"] = "ok"
    except Exception as e:  # noqa: BLE001
        checks["redis"] = f"error: {type(e).__name__}"
    if any(v != "ok" for v in checks.values()):
        response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE
    return checks


@app.get("/v1/metrics")
def metrics(caller: Caller = Depends(get_caller)) -> dict:
    with session_scope() as s:
        by_status = dict(s.execute(select(WorkflowRun.status, func.count())
                                   .where(WorkflowRun.tenant_id == caller.tenant_id)
                                   .group_by(WorkflowRun.status)).all())
        interrupted = dict(s.execute(select(WorkflowRun.status, func.count())
                                     .where(WorkflowRun.tenant_id == caller.tenant_id,
                                            WorkflowRun.recovery_count > 0)
                                     .group_by(WorkflowRun.status)).all())
        latency = s.execute(text("""
            SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY extract(epoch FROM finished_at - created_at)),
                   percentile_cont(0.95) WITHIN GROUP (ORDER BY extract(epoch FROM finished_at - created_at))
            FROM workflow_runs WHERE tenant_id = :t AND status = 'SUCCEEDED'
              AND finished_at > now() - interval '1 hour'"""), {"t": caller.tenant_id}).first()
    total_interrupted = sum(interrupted.values())
    try:
        queue = RunQueue().stats()
    except Exception as e:  # noqa: BLE001
        queue = {"error": type(e).__name__}
    return {
        "runs_by_status": by_status,
        "recovery": {
            "interrupted_runs": total_interrupted,
            "recovered_succeeded": interrupted.get("SUCCEEDED", 0),
            "interrupted_by_final_status": interrupted,
        },
        "latency_seconds_last_hour": {"p50": latency[0], "p95": latency[1]},
        "queue": queue,
    }
