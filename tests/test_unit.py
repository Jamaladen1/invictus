"""Unit tests: no PostgreSQL or Redis needed."""

import pytest

from invictus.engine import backoff_delay
from invictus.errors import FatalError, RetryableError, is_retryable, redact
from invictus.spec import SpecError, topological_order, validate
from invictus.templating import references, render


def node(id, type="transform", **config):
    return {"id": id, "type": type, "config": config or {"template": "x"}}


# ---------------------------------------------------------------- DAG validation


def test_valid_dag_and_required_inputs():
    spec = validate({
        "name": "ok",
        "nodes": [node("a", template="{{input.doc}}"), node("b", template="{{nodes.a.text}} {{input.lang}}")],
        "edges": [{"source": "a", "target": "b"}],
    })
    assert spec.required_inputs() == {"doc", "lang"}


@pytest.mark.parametrize("graph, needle", [
    ({"nodes": [node("a"), node("a")]}, "duplicate node id"),
    ({"nodes": [node("a", type="shell")]}, "not an allowed tool"),
    ({"nodes": [node("a")], "edges": [{"source": "a", "target": "zzz"}]}, "unknown node"),
    ({"nodes": [node("a")], "edges": [{"source": "a", "target": "a"}]}, "self-loop"),
    ({"nodes": [node("a"), node("b")],
      "edges": [{"source": "a", "target": "b"}, {"source": "b", "target": "a"}]}, "cycle"),
    ({"nodes": [node("a", template="{{nodes.b.text}}"), node("b")]}, "not upstream"),
    ({"nodes": [node("a", template="{{secrets.key}}")]}, "must start with input. or nodes."),
    ({"nodes": [{"id": "a", "type": "llm", "config": {}}]}, "prompt"),
    ({"nodes": []}, "nodes"),
    ({"nodes": [{"id": "bad id!", "type": "transform", "config": {"template": "x"}}]}, "id"),
])
def test_invalid_dags(graph, needle):
    with pytest.raises(SpecError) as e:
        validate({"name": "bad", **graph})
    assert any(needle in err for err in e.value.errors), e.value.errors


def test_topological_order():
    assert topological_order(["a", "b", "c"], [("a", "b"), ("b", "c")]) == ["a", "b", "c"]
    assert topological_order(["a", "b"], [("a", "b"), ("b", "a")]) is None


# ---------------------------------------------------------------- templating


def test_render_types_and_nesting():
    ctx = {"input": {"n": 3, "s": "hi"}, "nodes": {"a": {"items": [1, 2]}}}
    assert render("{{input.n}}", ctx) == 3                       # whole-token keeps type
    assert render("n={{ input.n }} s={{input.s}}", ctx) == "n=3 s=hi"
    assert render({"k": ["{{nodes.a.items.1}}"]}, ctx) == {"k": [2]}
    assert references({"a": "{{input.x}}", "b": ["{{nodes.y.z}}"]}) == ["input.x", "nodes.y.z"]
    with pytest.raises(KeyError):
        render("{{input.missing}}", ctx)


# ---------------------------------------------------------------- retry policy


def test_retry_classification():
    assert is_retryable(RetryableError())
    assert is_retryable(TimeoutError())
    assert is_retryable(ConnectionError())
    assert is_retryable(RuntimeError("unknown"))
    assert not is_retryable(FatalError())
    assert not is_retryable(KeyError("bad template"))
    assert not is_retryable(ValueError())


def test_backoff_is_capped_with_jitter():
    for attempt in range(1, 12):
        d = backoff_delay(attempt, base=1.0, cap=8.0)
        assert 0 <= d <= min(8.0, 2 ** (attempt - 1))


def test_redaction():
    out = redact("failed with sk-ant-abc123 and Authorization: Bearer xyz.789 api_key=hunter2")
    assert "sk-ant-abc123" not in out and "xyz.789" not in out and "hunter2" not in out
    assert out.count("[REDACTED]") == 3
