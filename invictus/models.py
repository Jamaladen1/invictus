"""PostgreSQL schema: the system of record for definitions, runs, attempts and approvals.

LangGraph's own checkpoint tables (checkpoints, checkpoint_writes, checkpoint_blobs) live in
the same database and are keyed by thread_id == run id.
"""

import enum
import uuid
from datetime import datetime

from sqlalchemy import (
    BigInteger,
    Boolean,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
    func,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


def new_id() -> str:
    return str(uuid.uuid4())


class Base(DeclarativeBase):
    pass


class RunStatus(str, enum.Enum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    WAITING_APPROVAL = "WAITING_APPROVAL"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    REJECTED = "REJECTED"
    CANCELLED = "CANCELLED"


TERMINAL_STATUSES = {RunStatus.SUCCEEDED, RunStatus.FAILED, RunStatus.REJECTED, RunStatus.CANCELLED}
ACTIVE_STATUSES = {RunStatus.PENDING, RunStatus.RUNNING, RunStatus.WAITING_APPROVAL}


class AttemptStatus(str, enum.Enum):
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    ABANDONED = "ABANDONED"          # worker died mid-attempt; does not count against retries
    WAITING_APPROVAL = "WAITING_APPROVAL"
    REJECTED = "REJECTED"


class WorkflowDefinition(Base):
    __tablename__ = "workflow_definitions"
    __table_args__ = (UniqueConstraint("tenant_id", "name", "version"),)

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_id)
    tenant_id: Mapped[str] = mapped_column(String(64), index=True)
    name: Mapped[str] = mapped_column(String(200))
    version: Mapped[int] = mapped_column(Integer)
    graph_json: Mapped[dict] = mapped_column(JSONB)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


class WorkflowRun(Base):
    __tablename__ = "workflow_runs"
    __table_args__ = (
        UniqueConstraint("tenant_id", "idempotency_key"),
        Index("ix_runs_tenant_status", "tenant_id", "status"),
        Index("ix_runs_status_lease", "status", "lease_expires_at"),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_id)
    tenant_id: Mapped[str] = mapped_column(String(64))
    definition_id: Mapped[str] = mapped_column(ForeignKey("workflow_definitions.id"))
    status: Mapped[str] = mapped_column(String(32), default=RunStatus.PENDING.value)
    input_json: Mapped[dict] = mapped_column(JSONB, default=dict)
    output_json: Mapped[dict | None] = mapped_column(JSONB, nullable=True)
    error: Mapped[str | None] = mapped_column(Text, nullable=True)
    idempotency_key: Mapped[str | None] = mapped_column(String(200), nullable=True)
    cancel_requested: Mapped[bool] = mapped_column(Boolean, default=False)
    lease_owner: Mapped[str | None] = mapped_column(String(200), nullable=True)
    lease_expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    recovery_count: Mapped[int] = mapped_column(Integer, default=0)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)


class NodeAttempt(Base):
    __tablename__ = "node_attempts"
    __table_args__ = (UniqueConstraint("run_id", "node_id", "attempt"),)

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    run_id: Mapped[str] = mapped_column(ForeignKey("workflow_runs.id", ondelete="CASCADE"), index=True)
    node_id: Mapped[str] = mapped_column(String(100))
    attempt: Mapped[int] = mapped_column(Integer)
    status: Mapped[str] = mapped_column(String(32))
    output_json: Mapped[dict | None] = mapped_column(JSONB, nullable=True)
    error: Mapped[str | None] = mapped_column(Text, nullable=True)
    retryable: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    worker_id: Mapped[str | None] = mapped_column(String(200), nullable=True)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)


class ApprovalEvent(Base):
    __tablename__ = "approval_events"
    # One authoritative decision per approval gate.
    __table_args__ = (UniqueConstraint("run_id", "node_id"),)

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    run_id: Mapped[str] = mapped_column(ForeignKey("workflow_runs.id", ondelete="CASCADE"))
    node_id: Mapped[str] = mapped_column(String(100))
    actor_id: Mapped[str] = mapped_column(String(200))
    decision: Mapped[str] = mapped_column(String(16))  # "approve" | "reject"
    comment: Mapped[str | None] = mapped_column(Text, nullable=True)
    decided_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


class OutboxEvent(Base):
    """Transactional outbox: written in the same transaction as the run change it announces."""

    __tablename__ = "outbox"
    __table_args__ = (Index("ix_outbox_unpublished", "id", postgresql_where="published_at IS NULL"),)

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    run_id: Mapped[str] = mapped_column(String(36))
    kind: Mapped[str] = mapped_column(String(32))  # start | resume | sweep
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    published_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)


class AuditLog(Base):
    __tablename__ = "audit_log"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    tenant_id: Mapped[str] = mapped_column(String(64), index=True)
    actor_id: Mapped[str] = mapped_column(String(200))
    action: Mapped[str] = mapped_column(String(64))
    run_id: Mapped[str | None] = mapped_column(String(36), nullable=True)
    detail: Mapped[dict] = mapped_column(JSONB, default=dict)
    at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


# --- Demo side-effect tables used by the mock tool to prove effectively-once behaviour ---


class ToolExecution(Base):
    """Every physical execution of a side-effecting mock call (at-least-once)."""

    __tablename__ = "tool_executions"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    idempotency_key: Mapped[str] = mapped_column(String(300), index=True)
    at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


class SideEffect(Base):
    """The externally visible effect, deduplicated by idempotency key (effectively-once)."""

    __tablename__ = "side_effects"

    idempotency_key: Mapped[str] = mapped_column(String(300), primary_key=True)
    payload: Mapped[dict] = mapped_column(JSONB, default=dict)
    at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
