"""HTTP tool: calls an allow-listed host and forwards a stable Idempotency-Key,
so a downstream that honours it sees each logical call once even if Invictus re-executes it."""

from typing import Any, Literal
from urllib.parse import urlparse

import httpx
from pydantic import BaseModel

from ..config import get_settings
from ..errors import FatalError, RetryableError
from ..templating import render
from . import Tool, ToolContext


class HttpConfig(BaseModel):
    method: Literal["GET", "POST", "PUT", "PATCH", "DELETE"] = "POST"
    url: str
    json_body: Any = None
    headers: dict[str, str] = {}


def run(ctx: ToolContext, cfg: HttpConfig) -> dict:
    url = render(cfg.url, ctx.template_context)
    host = (urlparse(url).hostname or "").lower()
    if host not in get_settings().allowed_http_hosts():
        raise FatalError(f"host '{host}' is not in INVICTUS_HTTP_ALLOWED_HOSTS")
    headers = {**render(cfg.headers, ctx.template_context), "Idempotency-Key": ctx.idempotency_key}
    body = render(cfg.json_body, ctx.template_context) if cfg.json_body is not None else None
    try:
        resp = httpx.request(cfg.method, url, json=body, headers=headers, timeout=ctx.deadline_seconds)
    except httpx.TimeoutException as e:
        raise RetryableError("http timeout") from e
    except httpx.TransportError as e:
        raise RetryableError(f"http transport error: {type(e).__name__}") from e
    if resp.status_code == 429 or resp.status_code >= 500:
        raise RetryableError(f"http {resp.status_code}")
    if resp.status_code >= 400:
        raise FatalError(f"http {resp.status_code}: {resp.text[:300]}")
    try:
        data = resp.json()
    except ValueError:
        data = resp.text
    return {"status": resp.status_code, "body": data}


TOOL = Tool(name="http", config_model=HttpConfig, run=run, side_effecting=True)
