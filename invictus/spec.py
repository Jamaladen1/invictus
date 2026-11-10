"""Workflow definition schema and DAG validation."""

from collections import defaultdict, deque
from typing import Any

from pydantic import BaseModel, Field, ValidationError

from .templating import references
from .tools import registry

NODE_ID_PATTERN = r"^[A-Za-z][A-Za-z0-9_\-]{0,99}$"


class RetryPolicy(BaseModel):
    max_attempts: int = Field(default=3, ge=1, le=20)
    backoff_seconds: float = Field(default=1.0, ge=0, le=300)
    max_backoff_seconds: float = Field(default=30.0, ge=0, le=3600)


class NodeSpec(BaseModel):
    id: str = Field(pattern=NODE_ID_PATTERN)
    type: str
    config: dict[str, Any] = {}
    retry: RetryPolicy = RetryPolicy()
    timeout_seconds: float = Field(default=120.0, gt=0, le=3600)


class EdgeSpec(BaseModel):
    source: str
    target: str


class WorkflowSpec(BaseModel):
    name: str = Field(min_length=1, max_length=200)
    description: str | None = None
    nodes: list[NodeSpec] = Field(min_length=1, max_length=200)
    edges: list[EdgeSpec] = []

    def parents(self) -> dict[str, list[str]]:
        p: dict[str, list[str]] = {n.id: [] for n in self.nodes}
        for e in self.edges:
            p[e.target].append(e.source)
        return p

    def children(self) -> dict[str, list[str]]:
        c: dict[str, list[str]] = {n.id: [] for n in self.nodes}
        for e in self.edges:
            c[e.source].append(e.target)
        return c

    def node(self, node_id: str) -> NodeSpec:
        return next(n for n in self.nodes if n.id == node_id)

    def required_inputs(self) -> set[str]:
        keys: set[str] = set()
        for n in self.nodes:
            for ref in references(n.config):
                head, _, rest = ref.partition(".")
                if head == "input" and rest:
                    keys.add(rest.split(".")[0])
        return keys


class SpecError(ValueError):
    def __init__(self, errors: list[str]):
        super().__init__("; ".join(errors))
        self.errors = errors


def topological_order(node_ids: list[str], edges: list[tuple[str, str]]) -> list[str] | None:
    """Kahn's algorithm. Returns None if the graph has a cycle."""
    indeg = {n: 0 for n in node_ids}
    out = defaultdict(list)
    for s, t in edges:
        out[s].append(t)
        indeg[t] += 1
    q = deque(n for n in node_ids if indeg[n] == 0)
    order = []
    while q:
        n = q.popleft()
        order.append(n)
        for t in out[n]:
            indeg[t] -= 1
            if indeg[t] == 0:
                q.append(t)
    return order if len(order) == len(node_ids) else None


def ancestors(spec: WorkflowSpec) -> dict[str, set[str]]:
    parents = spec.parents()
    memo: dict[str, set[str]] = {}

    def visit(n: str) -> set[str]:
        if n not in memo:
            memo[n] = set()
            for p in parents[n]:
                memo[n] |= {p} | visit(p)
        return memo[n]

    for n in parents:
        visit(n)
    return memo


def validate(raw: dict) -> WorkflowSpec:
    """Parse and validate a definition; raises SpecError with every problem found."""
    try:
        spec = WorkflowSpec.model_validate(raw)
    except ValidationError as e:
        raise SpecError([f"{'.'.join(map(str, err['loc']))}: {err['msg']}" for err in e.errors()]) from e

    errors: list[str] = []
    ids = [n.id for n in spec.nodes]
    seen: set[str] = set()
    for i in ids:
        if i in seen:
            errors.append(f"duplicate node id '{i}'")
        seen.add(i)

    tools = registry()
    for n in spec.nodes:
        tool = tools.get(n.type)
        if tool is None:
            errors.append(f"node '{n.id}': type '{n.type}' is not an allowed tool ({', '.join(sorted(tools))})")
            continue
        try:
            tool.config_model.model_validate(n.config)
        except ValidationError as e:
            for err in e.errors():
                errors.append(f"node '{n.id}' config.{'.'.join(map(str, err['loc']))}: {err['msg']}")

    edge_pairs: set[tuple[str, str]] = set()
    for e in spec.edges:
        for end in (e.source, e.target):
            if end not in seen:
                errors.append(f"edge {e.source}->{e.target}: unknown node '{end}'")
        if e.source == e.target:
            errors.append(f"edge {e.source}->{e.target}: self-loop")
        if (e.source, e.target) in edge_pairs:
            errors.append(f"duplicate edge {e.source}->{e.target}")
        edge_pairs.add((e.source, e.target))

    if errors:
        raise SpecError(errors)

    if topological_order(ids, list(edge_pairs)) is None:
        raise SpecError(["graph contains a cycle"])

    # Templates may only read outputs of upstream (ancestor) nodes.
    anc = ancestors(spec)
    for n in spec.nodes:
        for ref in references(n.config):
            head, _, rest = ref.partition(".")
            if head == "nodes":
                dep = rest.split(".")[0]
                if dep not in anc[n.id]:
                    errors.append(f"node '{n.id}' references nodes.{dep}, which is not upstream of it")
            elif head != "input":
                errors.append(f"node '{n.id}': template reference '{ref}' must start with input. or nodes.")
    if errors:
        raise SpecError(errors)
    return spec
