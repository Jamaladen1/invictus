"""Deterministic test tool for retries, timeouts and fault injection.

side_effect=True records every physical execution in tool_executions and the
deduplicated effect in side_effects, so experiments can measure duplicates.
"""

import time
from typing import Any

from pydantic import BaseModel
from sqlalchemy.dialects.postgresql import insert

from ..db import session_scope
from ..errors import FatalError, RetryableError
from ..models import SideEffect, ToolExecution
from ..templating import render
from . import Tool, ToolContext


class MockConfig(BaseModel):
    output: Any = None
    sleep_seconds: float = 0.0
    fail_times: int = 0          # fail the first N attempts with a retryable error
    fatal: bool = False
    side_effect: bool = False


def run(ctx: ToolContext, cfg: MockConfig) -> dict:
    if cfg.sleep_seconds:
        time.sleep(cfg.sleep_seconds)
    if cfg.fatal:
        raise FatalError("mock fatal error")
    if ctx.attempt <= cfg.fail_times:
        raise RetryableError(f"mock transient failure (attempt {ctx.attempt})")
    if cfg.side_effect:
        with session_scope() as s:
            s.add(ToolExecution(idempotency_key=ctx.idempotency_key))
            s.execute(
                insert(SideEffect)
                .values(idempotency_key=ctx.idempotency_key, payload={"run_id": ctx.run_id})
                .on_conflict_do_nothing()
            )
    return {"value": render(cfg.output, ctx.template_context), "attempt": ctx.attempt}


TOOL = Tool(name="mock", config_model=MockConfig, run=run, side_effecting=True)
