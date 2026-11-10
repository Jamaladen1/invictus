"""Compiles a workflow definition into a LangGraph StateGraph and executes it durably.

Durability layers:
  1. LangGraph checkpoints (PostgresSaver, thread_id = run id, durability="sync") record
     graph progress after every superstep, so a resumed run skips finished supersteps.
  2. node_attempts rows record each node's result before LangGraph checkpoints it. If a
     worker dies between the two, the resumed node finds its SUCCEEDED attempt and returns
     the stored output instead of re-running the tool.
  3. Side-effecting tools receive a stable idempotency key (run_id:node_id) for the narrow
     window where the tool ran but its result was not yet persisted.
"""

import logging
import random
import time
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import TimeoutError as FuturesTimeout
from dataclasses import dataclass, field
from typing import Annotated, Any, Callable, TypedDict

from langgraph.checkpoint.postgres import PostgresSaver
from langgraph.graph import END, START, StateGraph
from langgraph.types import interrupt
from sqlalchemy import func, select, text, update
from sqlalchemy.dialects.postgresql import insert

from . import faults
from .db import get_checkpoint_pool, session_scope
from .errors import ApprovalRejected, LeaseLost, NodeFailed, RunCancelled, is_retryable, redact
from .models import ApprovalEvent, AttemptStatus, NodeAttempt, RunStatus, WorkflowRun
from .spec import NodeSpec, WorkflowSpec
from .tools import ToolContext, registry

log = logging.getLogger("invictus.engine")


def _merge(a: dict | None, b: dict | None) -> dict:
    return {**(a or {}), **(b or {})}


class GraphState(TypedDict):
    input: dict
    outputs: Annotated[dict, _merge]


@dataclass
class RunContext:
    run_id: str
    worker_id: str
    recovery_count: int
    check_alive: Callable[[], None] = field(default=lambda: None)  # raises LeaseLost / RunCancelled


@dataclass
class Outcome:
    status: RunStatus
    output: dict | None = None
    error: str | None = None


def graph_node_name(node_id: str) -> str:
    # LangGraph forbids node names that collide with state keys ("input", "outputs").
    return f"node__{node_id}"


# ---------------------------------------------------------------- persistence helpers


def _succeeded_output(run_id: str, node_id: str) -> dict | None:
    with session_scope() as s:
        row = s.execute(
            select(NodeAttempt.output_json)
            .where(NodeAttempt.run_id == run_id, NodeAttempt.node_id == node_id,
                   NodeAttempt.status == AttemptStatus.SUCCEEDED.value)
            .limit(1)
        ).first()
        return row[0] if row else None


def _start_attempt(ctx: RunContext, node_id: str, max_attempts: int) -> tuple[int, int] | None:
    """Insert a RUNNING attempt. Returns (row id, logical attempt no), or None if the retry
    budget is already spent (e.g. failures recorded before a crash)."""
    with session_scope() as s:
        s.execute(text("SELECT pg_advisory_xact_lock(hashtext(:k))"), {"k": f"{ctx.run_id}:{node_id}"})
        max_attempt = s.scalar(
            select(func.coalesce(func.max(NodeAttempt.attempt), 0))
            .where(NodeAttempt.run_id == ctx.run_id, NodeAttempt.node_id == node_id)
        )
        failed = s.scalar(
            select(func.count()).select_from(NodeAttempt)
            .where(NodeAttempt.run_id == ctx.run_id, NodeAttempt.node_id == node_id,
                   NodeAttempt.status == AttemptStatus.FAILED.value)
        )
        if failed >= max_attempts:
            return None
        row = NodeAttempt(run_id=ctx.run_id, node_id=node_id, attempt=max_attempt + 1,
                          status=AttemptStatus.RUNNING.value, worker_id=ctx.worker_id)
        s.add(row)
        s.flush()
        return row.id, failed + 1


def _finish_attempt(ctx: RunContext, attempt_id: int, status: AttemptStatus, *, output: Any = None,
                    error: str | None = None, retryable: bool | None = None) -> None:
    """Lease-fenced write: only the current lease owner may record a result."""
    with session_scope() as s:
        owns = select(WorkflowRun.id).where(WorkflowRun.id == ctx.run_id,
                                            WorkflowRun.lease_owner == ctx.worker_id).exists()
        res = s.execute(
            update(NodeAttempt)
            .where(NodeAttempt.id == attempt_id, NodeAttempt.status == AttemptStatus.RUNNING.value, owns)
            .values(status=status.value, output_json=output, error=error, retryable=retryable,
                    finished_at=func.now())
        )
        if res.rowcount == 0:
            raise LeaseLost(f"lost lease on run {ctx.run_id} while finishing attempt {attempt_id}")


def _failed_errors(run_id: str, node_id: str) -> list[str]:
    with session_scope() as s:
        return [e for (e,) in s.execute(
            select(NodeAttempt.error).where(NodeAttempt.run_id == run_id, NodeAttempt.node_id == node_id,
                                            NodeAttempt.status == AttemptStatus.FAILED.value)
            .order_by(NodeAttempt.attempt)
        )]


# ---------------------------------------------------------------- node execution


def backoff_delay(failed_attempts: int, base: float, cap: float) -> float:
    """Capped exponential backoff with full jitter."""
    return random.uniform(0, min(cap, base * (2 ** max(0, failed_attempts - 1))))


def _run_with_timeout(fn: Callable, timeout: float, *args) -> Any:
    pool = ThreadPoolExecutor(max_workers=1, thread_name_prefix="invictus-tool")
    try:
        return pool.submit(fn, *args).result(timeout=timeout)
    except FuturesTimeout as e:
        # The tool thread cannot be killed; it is abandoned and its result ignored.
        raise TimeoutError(f"node exceeded timeout of {timeout}s") from e
    finally:
        pool.shutdown(wait=False, cancel_futures=True)


def _sleep_interruptibly(ctx: RunContext, seconds: float) -> None:
    end = time.monotonic() + seconds
    while (left := end - time.monotonic()) > 0:
        time.sleep(min(0.5, left))
        ctx.check_alive()


def _execute_approval(ctx: RunContext, node: NodeSpec) -> dict:
    with session_scope() as s:
        event = s.scalar(select(ApprovalEvent).where(ApprovalEvent.run_id == ctx.run_id,
                                                     ApprovalEvent.node_id == node.id))
        if event is None:
            s.execute(insert(NodeAttempt).values(
                run_id=ctx.run_id, node_id=node.id, attempt=1,
                status=AttemptStatus.WAITING_APPROVAL.value, worker_id=ctx.worker_id,
            ).on_conflict_do_nothing())
    if event is None:
        # Raises GraphInterrupt: LangGraph checkpoints and stops; the worker parks the run.
        interrupt({"node_id": node.id, "message": node.config.get("message", "Approval required")})

    approved = event.decision == "approve"
    output = {"decision": event.decision, "actor": event.actor_id, "comment": event.comment}
    with session_scope() as s:
        s.execute(update(NodeAttempt)
                  .where(NodeAttempt.run_id == ctx.run_id, NodeAttempt.node_id == node.id,
                         NodeAttempt.status == AttemptStatus.WAITING_APPROVAL.value)
                  .values(status=(AttemptStatus.SUCCEEDED if approved else AttemptStatus.REJECTED).value,
                          output_json=output, finished_at=func.now()))
    if not approved:
        raise ApprovalRejected(node.id, event.actor_id, event.comment)
    return output


def execute_node(ctx: RunContext, node: NodeSpec, state: GraphState) -> dict:
    ctx.check_alive()

    done = _succeeded_output(ctx.run_id, node.id)
    if done is not None:
        log.info("run %s node %s: reusing persisted result (recovery replay)", ctx.run_id, node.id)
        return done

    if node.type == "approval":
        return _execute_approval(ctx, node)

    tool = registry()[node.type]
    cfg = tool.config_model.model_validate(node.config)
    template_context = {"input": state.get("input", {}), "nodes": state.get("outputs", {})}

    while True:
        ctx.check_alive()
        started = _start_attempt(ctx, node.id, node.retry.max_attempts)
        if started is None:
            errs = _failed_errors(ctx.run_id, node.id)
            raise NodeFailed(node.id, errs[-1] if errs else "retry budget exhausted")
        attempt_id, logical = started

        faults.maybe_crash("before_execute", node.id, ctx.recovery_count)
        tctx = ToolContext(run_id=ctx.run_id, node_id=node.id, attempt=logical,
                           idempotency_key=f"{ctx.run_id}:{node.id}",
                           template_context=template_context, deadline_seconds=node.timeout_seconds)
        try:
            output = _run_with_timeout(tool.run, node.timeout_seconds, tctx, cfg)
        except (LeaseLost, RunCancelled):
            raise
        except Exception as e:  # noqa: BLE001 - classify every tool failure
            retryable = is_retryable(e)
            msg = redact(f"{type(e).__name__}: {e}")
            _finish_attempt(ctx, attempt_id, AttemptStatus.FAILED, error=msg, retryable=retryable)
            log.warning("run %s node %s attempt %d failed (retryable=%s): %s",
                        ctx.run_id, node.id, logical, retryable, msg)
            if not retryable or logical >= node.retry.max_attempts:
                raise NodeFailed(node.id, msg) from e
            _sleep_interruptibly(ctx, backoff_delay(logical, node.retry.backoff_seconds,
                                                    node.retry.max_backoff_seconds))
            continue

        faults.maybe_crash("after_side_effect", node.id, ctx.recovery_count)
        _finish_attempt(ctx, attempt_id, AttemptStatus.SUCCEEDED, output=output)
        faults.maybe_crash("after_persist", node.id, ctx.recovery_count)
        return output


# ---------------------------------------------------------------- graph compile & run


def build_graph(spec: WorkflowSpec, ctx: RunContext, checkpointer) -> Any:
    builder = StateGraph(GraphState)
    parents = spec.parents()
    children = spec.children()

    for node in spec.nodes:
        def fn(state: GraphState, _node: NodeSpec = node) -> dict:
            return {"outputs": {_node.id: execute_node(ctx, _node, state)}}
        builder.add_node(graph_node_name(node.id), fn)

    for node in spec.nodes:
        name = graph_node_name(node.id)
        ps = [graph_node_name(p) for p in parents[node.id]]
        if not ps:
            builder.add_edge(START, name)
        elif len(ps) == 1:
            builder.add_edge(ps[0], name)
        else:
            builder.add_edge(ps, name)  # fan-in: waits for every parent
        if not children[node.id]:
            builder.add_edge(name, END)

    return builder.compile(checkpointer=checkpointer)


def run_workflow(spec: WorkflowSpec, run_input: dict, ctx: RunContext) -> Outcome:
    """Start or resume a run from its latest checkpoint. Raises run-level exceptions."""
    checkpointer = PostgresSaver(get_checkpoint_pool())
    graph = build_graph(spec, ctx, checkpointer)
    config = {"configurable": {"thread_id": ctx.run_id},
              "recursion_limit": max(25, 2 * len(spec.nodes) + 5)}

    snapshot = graph.get_state(config)
    fresh = snapshot.created_at is None
    if not fresh:
        log.info("run %s: resuming from checkpoint (pending=%s)", ctx.run_id, snapshot.next)
    graph.invoke({"input": run_input, "outputs": {}} if fresh else None, config, durability="sync")

    snapshot = graph.get_state(config)
    if snapshot.interrupts or any(t.interrupts for t in snapshot.tasks):
        return Outcome(RunStatus.WAITING_APPROVAL)
    if snapshot.next:
        return Outcome(RunStatus.FAILED, error=f"graph stopped with pending nodes {snapshot.next}")

    outputs = snapshot.values.get("outputs", {})
    sinks = [n.id for n in spec.nodes if not spec.children()[n.id]]
    return Outcome(RunStatus.SUCCEEDED, output={"result": {k: outputs.get(k) for k in sinks}, "nodes": outputs})
