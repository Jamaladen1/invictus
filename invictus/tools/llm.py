"""LLM node: renders a prompt and calls a model adapter.

Adapters:
  fake       deterministic, offline (default; used by tests and the recovery experiment)
  anthropic  Claude via the official Anthropic SDK
"""

import hashlib

from pydantic import BaseModel

from ..config import get_settings
from ..errors import FatalError, RetryableError
from ..templating import render
from . import Tool, ToolContext


class LLMConfig(BaseModel):
    prompt: str
    system: str | None = None
    model: str | None = None
    max_tokens: int = 16000
    effort: str | None = None  # low | medium | high | xhigh | max


def _fake(prompt: str, system: str | None, model: str) -> dict:
    digest = hashlib.sha256(((system or "") + prompt).encode()).hexdigest()[:12]
    return {"text": f"[fake:{digest}] {prompt[:500]}", "model": "fake"}


def _anthropic(prompt: str, system: str | None, cfg: LLMConfig, model: str, timeout: float) -> dict:
    import anthropic

    client = anthropic.Anthropic(timeout=timeout, max_retries=0)  # Invictus owns retries
    kwargs = {}
    if system:
        kwargs["system"] = system
    if cfg.effort:
        kwargs["output_config"] = {"effort": cfg.effort}
    try:
        # Server-side fallback reroutes a safety refusal to another model inside the same call.
        resp = client.beta.messages.create(
            model=model,
            max_tokens=cfg.max_tokens,
            messages=[{"role": "user", "content": prompt}],
            betas=["server-side-fallback-2026-07-01"],
            fallbacks="default",
            **kwargs,
        )
    except (anthropic.RateLimitError, anthropic.InternalServerError, anthropic.APIConnectionError) as e:
        raise RetryableError(f"anthropic transient error: {type(e).__name__}") from e
    except anthropic.APIStatusError as e:
        if e.status_code in (408, 409, 529):
            raise RetryableError(f"anthropic status {e.status_code}") from e
        raise FatalError(f"anthropic status {e.status_code}: {e.message}") from e

    if resp.stop_reason == "refusal":
        raise FatalError("model declined the request (stop_reason=refusal)")
    text = "".join(b.text for b in resp.content if b.type == "text")
    return {
        "text": text,
        "model": resp.model,
        "stop_reason": resp.stop_reason,
        "usage": {"input_tokens": resp.usage.input_tokens, "output_tokens": resp.usage.output_tokens},
    }


def run(ctx: ToolContext, cfg: LLMConfig) -> dict:
    settings = get_settings()
    prompt = render(cfg.prompt, ctx.template_context)
    if not isinstance(prompt, str):
        prompt = str(prompt)
    system = render(cfg.system, ctx.template_context) if cfg.system else None
    model = cfg.model or settings.llm_model
    if settings.llm_provider == "anthropic":
        return _anthropic(prompt, system, cfg, model, ctx.deadline_seconds)
    if settings.llm_provider == "fake":
        return _fake(prompt, system, model)
    raise FatalError(f"unknown llm provider '{settings.llm_provider}'")


TOOL = Tool(name="llm", config_model=LLMConfig, run=run)
