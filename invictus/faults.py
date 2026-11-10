"""Deterministic fault injection for recovery testing.

INVICTUS_FAULTS="node_id:point[:max_recoveries],..."  (node_id may be "*")

Points (in execution order for one node):
  before_execute     attempt row written, tool not yet called      (pre-side-effect)
  after_side_effect  tool returned, result not yet persisted       (post-side-effect, pre-checkpoint)
  after_persist      node result persisted, LangGraph step not yet checkpointed

A fault fires only while the run's recovery_count < max_recoveries (default 1), so the
recovered worker proceeds instead of crash-looping. The process exits with os._exit,
which is as abrupt as SIGKILL: no cleanup, no lease release, no ack.
"""

import logging
import os

from .config import get_settings

POINTS = {"before_execute", "after_side_effect", "after_persist"}
log = logging.getLogger("invictus.faults")


def _parse(spec: str) -> list[tuple[str, str, int]]:
    out = []
    for item in filter(None, (s.strip() for s in spec.split(","))):
        parts = item.split(":")
        if len(parts) >= 2 and parts[1] in POINTS:
            out.append((parts[0], parts[1], int(parts[2]) if len(parts) > 2 else 1))
    return out


def maybe_crash(point: str, node_id: str, recovery_count: int) -> None:
    spec = get_settings().faults
    if not spec:
        return
    for node, pt, max_rec in _parse(spec):
        if pt == point and node in (node_id, "*") and recovery_count < max_rec:
            log.warning("FAULT INJECTION: killing worker at %s of node %s", point, node_id)
            os._exit(137)
