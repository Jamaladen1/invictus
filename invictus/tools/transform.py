from typing import Any

from pydantic import BaseModel

from ..templating import render
from . import Tool, ToolContext


class TransformConfig(BaseModel):
    # A template string, or any JSON structure containing templates.
    template: Any


def run(ctx: ToolContext, cfg: TransformConfig) -> dict:
    value = render(cfg.template, ctx.template_context)
    return {"text": value} if isinstance(value, str) else {"value": value}


TOOL = Tool(name="transform", config_model=TransformConfig, run=run)
