"""Worker process: claims runs from Redis, holds a PostgreSQL lease, executes, records outcome.

    python -m invictus.worker [--concurrency N]

Ownership rule: Redis decides who *might* work on a run; the PostgreSQL lease decides who
*does*. A worker acquires the lease only if it is free or expired, renews it on a heartbeat,
and every result write is fenced on still owning it.
"""

import argparse
import logging
import os
import signal
import socket
import threading
import time
import uuid
from dataclasses import dataclass

from sqlalchemy import select, text, update

from .config import get_settings
from .db import init_db, session_scope
from .engine import Outcome, RunContext, run_workflow
from .errors import ApprovalRejected, LeaseLost, NodeFailed, RunCancelled, redact
from .models import (
    TERMINAL_STATUSES,
    AttemptStatus,
    AuditLog,
    NodeAttempt,
    OutboxEvent,
    RunStatus,
    WorkflowDefinition,
    WorkflowRun,
)
from .queue import Message, RunQueue
from .spec import WorkflowSpec

log = logging.getLogger("invictus.worker")

_ACQUIRE_SQL = text("""
WITH prev AS (
    SELECT id, lease_owner AS prev_owner, lease_expires_at AS prev_expires_at
    FROM workflow_runs WHERE id = :run_id FOR UPDATE
)
UPDATE workflow_runs r SET
    lease_owner = :owner,
    lease_expires_at = now() + make_interval(secs => :lease),
    recovery_count = r.recovery_count + CASE WHEN prev.prev_owner IS NOT NULL THEN 1 ELSE 0 END,
    status = 'RUNNING',
    started_at = COALESCE(r.started_at, now()),
    updated_at = now()
FROM prev
WHERE r.id = prev.id
  AND r.status IN ('PENDING', 'RUNNING', 'WAITING_APPROVAL')
  AND (r.lease_owner IS NULL OR r.lease_expires_at < now())
RETURNING r.recovery_count, prev.prev_owner, r.cancel_requested, r.tenant_id, prev.prev_expires_at
""")


@dataclass
class Claim:
    recovery_count: int
    took_over_from: str | None
    cancel_requested: bool


def acquire_lease(run_id: str, owner: str, lease_seconds: float) -> Claim | None:
    with session_scope() as s:
        row = s.execute(_ACQUIRE_SQL, {"run_id": run_id, "owner": owner, "lease": lease_seconds}).first()
        if row is None:
            return None
        claim = Claim(row[0], row[1], row[2])
        if claim.took_over_from:
            # Attempts the dead worker left RUNNING never finished; they don't consume retries.
            abandoned = s.execute(
                update(NodeAttempt)
                .where(NodeAttempt.run_id == run_id, NodeAttempt.status == AttemptStatus.RUNNING.value)
                .values(status=AttemptStatus.ABANDONED.value, error="worker lost (lease expired)")
                .returning(NodeAttempt.node_id, NodeAttempt.attempt)).all()
            s.add(AuditLog(
                tenant_id=row[3], actor_id=f"worker:{owner}", action="run.recovered", run_id=run_id,
                detail={"previous_owner": claim.took_over_from,
                        "previous_lease_expired_at": row[4].isoformat() if row[4] else None,
                        "recovery_count": claim.recovery_count,
                        "abandoned_attempts": [{"node_id": n, "attempt": a} for n, a in abandoned]}))
        return claim


_DECIDED_BUT_UNAPPLIED = text("""
SELECT EXISTS (
    SELECT 1 FROM approval_events a
    JOIN node_attempts n ON n.run_id = a.run_id AND n.node_id = a.node_id
    WHERE a.run_id = :run_id AND n.status = 'WAITING_APPROVAL')
""")


def finalize(run_id: str, owner: str, outcome: Outcome) -> bool:
    """Write the outcome and release the lease, only if we still own it."""
    with session_scope() as s:
        # Lock the row: approve/cancel also lock it, so each either lands before this
        # (and is handled below) or after (and sees the lease released).
        run = s.scalar(select(WorkflowRun).where(WorkflowRun.id == run_id, WorkflowRun.lease_owner == owner)
                       .with_for_update())
        if run is None:
            return False
        if outcome.status == RunStatus.WAITING_APPROVAL:
            if run.cancel_requested:
                # Cancelled while the graph was pausing: nobody else would act on it.
                outcome = Outcome(RunStatus.CANCELLED, error="cancelled")
            elif s.scalar(_DECIDED_BUT_UNAPPLIED, {"run_id": run_id}):
                # Approved while we still held the lease; that resume message was dropped.
                s.add(OutboxEvent(run_id=run_id, kind="resume"))
        return _write_outcome(s, run_id, owner, outcome)


def _write_outcome(s, run_id: str, owner: str, outcome: Outcome) -> bool:
    values: dict = {"lease_owner": None, "lease_expires_at": None, "status": outcome.status.value}
    if outcome.status in TERMINAL_STATUSES:
        values["finished_at"] = text("now()")
    if outcome.output is not None:
        values["output_json"] = outcome.output
    if outcome.error is not None:
        values["error"] = redact(outcome.error)
    res = s.execute(update(WorkflowRun)
                    .where(WorkflowRun.id == run_id, WorkflowRun.lease_owner == owner)
                    .values(**values))
    if res.rowcount and outcome.status in TERMINAL_STATUSES:
        tenant = s.scalar(select(WorkflowRun.tenant_id).where(WorkflowRun.id == run_id))
        s.add(AuditLog(tenant_id=tenant, actor_id=f"worker:{owner}", action=f"run.{outcome.status.value.lower()}",
                       run_id=run_id, detail={"error": values.get("error")}))
    return bool(res.rowcount)


def release_lease(run_id: str, owner: str, error: str | None = None) -> None:
    values: dict = {"lease_owner": None, "lease_expires_at": None}
    if error:
        values["error"] = redact(error)
    with session_scope() as s:
        s.execute(update(WorkflowRun).where(WorkflowRun.id == run_id, WorkflowRun.lease_owner == owner)
                  .values(**values))


def fail_run_unowned(run_id: str, reason: str) -> None:
    with session_scope() as s:
        s.execute(update(WorkflowRun)
                  .where(WorkflowRun.id == run_id,
                         WorkflowRun.status.in_([st.value for st in RunStatus if st not in TERMINAL_STATUSES]))
                  .values(status=RunStatus.FAILED.value, error=reason, finished_at=text("now()"),
                          lease_owner=None, lease_expires_at=None))


class Heartbeat:
    """Renews the DB lease and the Redis message idle timer; flags loss of ownership."""

    def __init__(self, queue: RunQueue, consumer: str, msg: Message, run_id: str, owner: str, lease: float):
        self.queue, self.consumer, self.msg = queue, consumer, msg
        self.run_id, self.owner, self.lease = run_id, owner, lease
        self.lost = threading.Event()
        self.cancelled = threading.Event()
        self._stop = threading.Event()
        self._last_ok = time.monotonic()
        self._thread = threading.Thread(target=self._loop, daemon=True, name=f"hb-{run_id[:8]}")

    def __enter__(self):
        self._thread.start()
        return self

    def __exit__(self, *exc):
        self._stop.set()
        self._thread.join(timeout=5)

    def _loop(self) -> None:
        interval = max(0.2, self.lease / 3)
        while not self._stop.wait(interval):
            try:
                with session_scope() as s:
                    row = s.execute(text(
                        "UPDATE workflow_runs SET lease_expires_at = now() + make_interval(secs => :lease) "
                        "WHERE id = :id AND lease_owner = :owner RETURNING cancel_requested"),
                        {"lease": self.lease, "id": self.run_id, "owner": self.owner}).first()
                if row is None:
                    self.lost.set()
                    return
                if row[0]:
                    self.cancelled.set()
                self._last_ok = time.monotonic()
            except Exception as e:  # noqa: BLE001
                log.warning("heartbeat db error for run %s: %s", self.run_id, e)
                if time.monotonic() - self._last_ok > self.lease:
                    self.lost.set()
                    return
            try:
                self.queue.touch(self.consumer, self.msg.id)
            except Exception as e:  # noqa: BLE001 - Redis blip; the DB lease is authoritative
                log.warning("heartbeat redis error for run %s: %s", self.run_id, e)

    def check(self) -> None:
        if self.lost.is_set():
            raise LeaseLost(f"lease lost for run {self.run_id}")
        if self.cancelled.is_set():
            raise RunCancelled()


def process_message(queue: RunQueue, consumer: str, msg: Message) -> None:
    settings = get_settings()

    if msg.deliveries > settings.max_deliveries:
        reason = f"dead-lettered after {msg.deliveries} deliveries"
        fail_run_unowned(msg.run_id, reason)
        queue.dead_letter(msg, reason)
        log.error("run %s %s", msg.run_id, reason)
        return

    claim = acquire_lease(msg.run_id, consumer, settings.lease_seconds)
    if claim is None:
        # Run is terminal, unknown, or another live worker owns it (and holds its own message).
        queue.ack(msg.id)
        return

    if claim.took_over_from:
        log.warning("run %s: recovered from %s (recovery #%d)", msg.run_id, claim.took_over_from,
                    claim.recovery_count)
        if claim.recovery_count > settings.max_run_recoveries:
            finalize(msg.run_id, consumer, Outcome(RunStatus.FAILED,
                     error=f"exceeded recovery budget ({settings.max_run_recoveries} worker losses)"))
            queue.ack(msg.id)
            return

    if claim.cancel_requested:
        finalize(msg.run_id, consumer, Outcome(RunStatus.CANCELLED, error="cancelled"))
        queue.ack(msg.id)
        return

    with session_scope() as s:
        run = s.get(WorkflowRun, msg.run_id)
        definition = s.get(WorkflowDefinition, run.definition_id)
        spec = WorkflowSpec.model_validate(definition.graph_json)
        run_input = run.input_json or {}

    with Heartbeat(queue, consumer, msg, msg.run_id, consumer, settings.lease_seconds) as hb:
        ctx = RunContext(run_id=msg.run_id, worker_id=consumer, recovery_count=claim.recovery_count,
                         check_alive=hb.check)
        try:
            outcome = run_workflow(spec, run_input, ctx)
        except LeaseLost:
            log.warning("run %s: lease lost; abandoning without writing results", msg.run_id)
            return  # don't ack: whoever owns the run now (or a reclaim) will handle it
        except RunCancelled:
            outcome = Outcome(RunStatus.CANCELLED, error="cancelled")
        except ApprovalRejected as e:
            outcome = Outcome(RunStatus.REJECTED, error=str(e))
        except NodeFailed as e:
            outcome = Outcome(RunStatus.FAILED, error=str(e))
        except Exception as e:  # noqa: BLE001 - infrastructure error: retry via redelivery
            log.exception("run %s: infrastructure error; releasing for redelivery", msg.run_id)
            release_lease(msg.run_id, consumer, error=f"infrastructure error: {type(e).__name__}: {e}")
            return

    if finalize(msg.run_id, consumer, outcome):
        log.info("run %s -> %s", msg.run_id, outcome.status.value)
        queue.ack(msg.id)
    else:
        # Lost the lease at the last moment. The new owner may be working from this same
        # message id (reclaimed), so leave it pending rather than ack it out from under them.
        log.warning("run %s: lease lost before finalize; result discarded", msg.run_id)


class Worker:
    def __init__(self, concurrency: int | None = None, worker_id: str | None = None):
        s = get_settings()
        self.concurrency = concurrency or s.worker_concurrency
        self.worker_id = worker_id or f"{socket.gethostname()}-{os.getpid()}-{uuid.uuid4().hex[:6]}"
        self.stop_event = threading.Event()
        self.queue = RunQueue()

    def _loop(self, consumer: str) -> None:
        block = get_settings().poll_block_ms
        while not self.stop_event.is_set():
            try:
                msg = self.queue.reclaim(consumer) or self.queue.read(consumer, block)
                if msg is not None:
                    process_message(self.queue, consumer, msg)
            except Exception:  # noqa: BLE001
                log.exception("worker loop error (consumer %s)", consumer)
                self.stop_event.wait(1.0)

    def run(self) -> None:
        self.queue.ensure_group()
        threads = [threading.Thread(target=self._loop, args=(f"{self.worker_id}-t{i}",), daemon=True)
                   for i in range(self.concurrency)]
        for t in threads:
            t.start()
        log.info("worker %s started with concurrency %d", self.worker_id, self.concurrency)
        try:
            while any(t.is_alive() for t in threads):
                for t in threads:
                    t.join(timeout=0.5)
        finally:
            self.stop_event.set()

    def stop(self) -> None:
        """Graceful: finish in-flight runs, take no new ones."""
        self.stop_event.set()


def main() -> None:
    parser = argparse.ArgumentParser(description="Invictus worker")
    parser.add_argument("--concurrency", type=int, default=None)
    args = parser.parse_args()
    logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"),
                        format="%(asctime)s %(levelname)s %(name)s %(message)s")
    init_db()
    worker = Worker(concurrency=args.concurrency)
    signal.signal(signal.SIGTERM, lambda *_: worker.stop())
    signal.signal(signal.SIGINT, lambda *_: worker.stop())
    worker.run()


if __name__ == "__main__":
    main()
