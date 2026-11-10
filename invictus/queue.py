"""Redis Streams work queue with consumer groups.

Semantics:
  * at-least-once: a message stays in the group's pending list until XACK.
  * visibility timeout: a message idle longer than `lease_ms` (its consumer stopped
    heartbeating) is reclaimed by another consumer via XAUTOCLAIM.
  * heartbeat: XCLAIM with min-idle 0 to ourselves resets the idle timer.
  * dead letter: messages delivered more than `max_deliveries` times go to a DLQ stream.

Redis is only the dispatch layer. PostgreSQL holds the authoritative run state, so losing
Redis data loses no committed work (the dispatcher's sweeper re-enqueues).
"""

import time
from dataclasses import dataclass
from functools import lru_cache

import redis

from .config import get_settings


@dataclass
class Message:
    id: str
    run_id: str
    kind: str
    deliveries: int = 1


@lru_cache
def get_redis() -> redis.Redis:
    return redis.Redis.from_url(get_settings().redis_url, decode_responses=True, health_check_interval=30)


class RunQueue:
    def __init__(self, client: redis.Redis | None = None):
        s = get_settings()
        self.r = client or get_redis()
        self.stream = s.stream_key
        self.dlq = s.dead_letter_key
        self.group = s.consumer_group
        self.lease_ms = int(s.lease_seconds * 1000)
        self.max_deliveries = s.max_deliveries

    def ensure_group(self) -> None:
        try:
            self.r.xgroup_create(self.stream, self.group, id="0", mkstream=True)
        except redis.ResponseError as e:
            if "BUSYGROUP" not in str(e):
                raise

    def enqueue(self, run_id: str, kind: str) -> str:
        return self.r.xadd(self.stream, {"run_id": run_id, "kind": kind, "ts": str(time.time())},
                           maxlen=1_000_000, approximate=True)

    @staticmethod
    def _to_msg(msg_id: str, fields: dict | None, deliveries: int = 1) -> Message | None:
        if not fields or "run_id" not in fields:
            return None
        return Message(id=msg_id, run_id=fields["run_id"], kind=fields.get("kind", "start"), deliveries=deliveries)

    def read(self, consumer: str, block_ms: int) -> Message | None:
        res = self.r.xreadgroup(self.group, consumer, {self.stream: ">"}, count=1, block=block_ms)
        if not res:
            return None
        _, entries = res[0]
        msg_id, fields = entries[0]
        return self._to_msg(msg_id, fields)

    def _deliveries(self, msg_id: str) -> int:
        info = self.r.xpending_range(self.stream, self.group, min=msg_id, max=msg_id, count=1)
        return int(info[0]["times_delivered"]) if info else 1

    def reclaim(self, consumer: str) -> Message | None:
        """Take over one message whose consumer stopped heartbeating."""
        res = self.r.xautoclaim(self.stream, self.group, consumer, min_idle_time=self.lease_ms,
                                start_id="0-0", count=1)
        claimed = res[1] if len(res) > 1 else []
        for msg_id, fields in claimed:
            msg = self._to_msg(msg_id, fields, self._deliveries(msg_id))
            if msg is None:  # entry was trimmed or malformed
                self.ack(msg_id)
                continue
            return msg
        return None

    def touch(self, consumer: str, msg_id: str) -> None:
        self.r.xclaim(self.stream, self.group, consumer, min_idle_time=0, message_ids=[msg_id], justid=True)

    def ack(self, msg_id: str) -> None:
        pipe = self.r.pipeline()
        pipe.xack(self.stream, self.group, msg_id)
        pipe.xdel(self.stream, msg_id)
        pipe.execute()

    def dead_letter(self, msg: Message, reason: str) -> None:
        self.r.xadd(self.dlq, {"run_id": msg.run_id, "kind": msg.kind, "reason": reason[:500],
                               "deliveries": str(msg.deliveries), "ts": str(time.time())}, maxlen=100_000)
        self.ack(msg.id)

    def stats(self) -> dict:
        try:
            pending = self.r.xpending(self.stream, self.group)
            pending_count = pending["pending"] if isinstance(pending, dict) else pending[0]
        except redis.ResponseError:
            pending_count = 0
        return {
            "stream_length": self.r.xlen(self.stream),
            "pending": pending_count,
            "dead_letters": self.r.xlen(self.dlq),
        }
