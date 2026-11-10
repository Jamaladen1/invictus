"""Tool registry. Each node `type` maps to a Tool with a typed config model."""

from dataclasses import dataclass
from typing import Any, Callable

from pydantic import BaseModel

from ..config import get_settings


@dataclass
class ToolContext:
    run_id: str
    node_id: str
    attempt: int
    idempotency_key: str          # stable across retries and recoveries: "<run_id>:<node_id>"
    template_context: dict        # {"input": ..., "nodes": {id: output}}
    deadline_seconds: float


@dataclass
class Tool:
    name: str
    config_model: type[BaseModel]
    run: Callable[[ToolContext, Any], dict] | None  # None for engine-handled types (approval)
    side_effecting: bool = False


def registry() -> dict[str, Tool]:
    from . import approval, http, llm, mock, transform

    tools = {t.name: t for t in (llm.TOOL, transform.TOOL, http.TOOL, approval.TOOL)}
    if get_settings().enable_mock_tool:
        tools[mock.TOOL.name] = mock.TOOL
    return tools
