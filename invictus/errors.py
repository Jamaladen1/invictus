"""Error taxonomy used for retry classification and terminal run states."""

import re


class RetryableError(Exception):
    """Transient failure: the node may be retried with backoff."""


class FatalError(Exception):
    """Permanent failure: retrying cannot help (bad config, 4xx, refusal...)."""


class NodeFailed(Exception):
    def __init__(self, node_id: str, message: str):
        super().__init__(f"node '{node_id}' failed: {message}")
        self.node_id = node_id


class RunCancelled(Exception):
    pass


class ApprovalRejected(Exception):
    def __init__(self, node_id: str, actor: str, comment: str | None):
        super().__init__(f"approval '{node_id}' rejected by {actor}" + (f": {comment}" if comment else ""))
        self.node_id = node_id


class LeaseLost(Exception):
    """This worker no longer owns the run; stop without writing results."""


def is_retryable(exc: BaseException) -> bool:
    if isinstance(exc, FatalError):
        return False
    if isinstance(exc, (RetryableError, TimeoutError, ConnectionError)):
        return True
    if isinstance(exc, (ValueError, TypeError, KeyError)):
        return False
    # Unknown failures are treated as transient; the attempt budget bounds them.
    return True


_SECRET_PATTERNS = [
    re.compile(r"sk-ant-[A-Za-z0-9_\-]+"),
    re.compile(r"(?i)(bearer\s+)[A-Za-z0-9._\-]+"),
    re.compile(r"(?i)((?:api[_-]?key|token|password|secret)\s*[=:]\s*)[^\s,;]+"),
]


def redact(message: str, max_len: int = 2000) -> str:
    """Strip obvious secrets before an error is persisted or returned."""
    for pat in _SECRET_PATTERNS:
        message = pat.sub(lambda m: (m.group(1) if m.groups() else "") + "[REDACTED]", message)
    return message[:max_len]
