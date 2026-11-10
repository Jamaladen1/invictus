"""Tiny, safe templating: {{ input.key }} and {{ nodes.<id>.<field> }} with dotted paths.

No expressions or code execution, by design.
"""

import json
import re
from typing import Any

_TOKEN = re.compile(r"\{\{\s*([A-Za-z_][\w\-]*(?:\.[\w\-]+)*)\s*\}\}")


def references(template: Any) -> list[str]:
    """All dotted paths referenced anywhere inside a (possibly nested) template value."""
    if isinstance(template, str):
        return _TOKEN.findall(template)
    if isinstance(template, dict):
        return [r for v in template.values() for r in references(v)]
    if isinstance(template, list):
        return [r for v in template for r in references(v)]
    return []


def lookup(context: dict, path: str) -> Any:
    cur: Any = context
    for part in path.split("."):
        if isinstance(cur, dict) and part in cur:
            cur = cur[part]
        elif isinstance(cur, list) and part.isdigit() and int(part) < len(cur):
            cur = cur[int(part)]
        else:
            raise KeyError(f"template reference '{path}' not found")
    return cur


def render(template: Any, context: dict) -> Any:
    """Render strings; recurse into dicts/lists. A string that is exactly one token keeps its type."""
    if isinstance(template, dict):
        return {k: render(v, context) for k, v in template.items()}
    if isinstance(template, list):
        return [render(v, context) for v in template]
    if not isinstance(template, str):
        return template

    whole = _TOKEN.fullmatch(template.strip())
    if whole:
        return lookup(context, whole.group(1))

    def sub(m: re.Match) -> str:
        val = lookup(context, m.group(1))
        return val if isinstance(val, str) else json.dumps(val)

    return _TOKEN.sub(sub, template)
