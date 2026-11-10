"""Dispatcher: relays the transactional outbox to Redis and sweeps stuck runs.

    python -m invictus.dispatcher

Outbox relay: rows are inserted in the same transaction as the run change, so a committed
run can never be silently lost when Redis is down; publishing simply resumes later.
Publishing is at-least-once (a crash between XADD and marking the row published re-sends),
which workers tolerate through the lease check.

Sweeper: re-enqueues runs that should be progressing but nobody owns, e.g. after Redis
lost its data, or an approval whose resume message vanished.
"""

import logging
import os
import signal
import threading
import time

from sqlalchemy import text

from .config import get_settings
from .db import init_db, session_scope
from .queue import RunQueue

log = logging.getLogger("invictus.dispatcher")

_CLAIM_OUTBOX = text("""
SELECT id, run_id, kind FROM outbox
WHERE published_at IS NULL
ORDER BY id
LIMIT :limit
FOR UPDATE SKIP LOCKED
""")

_SWEEP = text("""
WITH stuck AS (
    SELECT r.id FROM workflow_runs r
    WHERE (
        (r.status IN ('PENDING', 'RUNNING') AND (r.lease_owner IS NULL OR r.lease_expires_at < now()))
        -- decided approval whose decision no worker has applied yet
        OR (r.status = 'WAITING_APPROVAL' AND r.lease_owner IS NULL AND EXISTS (
              SELECT 1 FROM approval_events a
              JOIN node_attempts n ON n.run_id = a.run_id AND n.node_id = a.node_id
              WHERE a.run_id = r.id AND n.status = 'WAITING_APPROVAL')))
      AND r.updated_at < now() - make_interval(secs => :stale)
      AND NOT EXISTS (SELECT 1 FROM outbox o WHERE o.run_id = r.id AND o.published_at IS NULL)
    LIMIT 500
    FOR UPDATE OF r SKIP LOCKED
), touched AS (
    UPDATE workflow_runs SET updated_at = now() WHERE id IN (SELECT id FROM stuck) RETURNING id
)
INSERT INTO outbox (run_id, kind) SELECT id, 'sweep' FROM touched
RETURNING run_id
""")


def relay_once(queue: RunQueue, limit: int = 200) -> int:
    with session_scope() as s:
        rows = s.execute(_CLAIM_OUTBOX, {"limit": limit}).all()
        for row in rows:
            queue.enqueue(row.run_id, row.kind)
        if rows:
            s.execute(text("UPDATE outbox SET published_at = now() WHERE id = ANY(:ids)"),
                      {"ids": [r.id for r in rows]})
        return len(rows)


def sweep_once(stale_seconds: float) -> int:
    with session_scope() as s:
        ids = s.execute(_SWEEP, {"stale": stale_seconds}).scalars().all()
    if ids:
        log.warning("sweeper re-enqueued %d stuck runs", len(ids))
    return len(ids)


class Dispatcher:
    def __init__(self):
        self.stop_event = threading.Event()
        self.queue = RunQueue()

    def run(self) -> None:
        settings = get_settings()
        self.queue.ensure_group()
        last_sweep = 0.0
        log.info("dispatcher started")
        while not self.stop_event.is_set():
            try:
                published = relay_once(self.queue)
                if time.monotonic() - last_sweep > settings.sweep_interval_seconds:
                    sweep_once(settings.sweep_stale_seconds)
                    last_sweep = time.monotonic()
                if published == 0:
                    self.stop_event.wait(settings.dispatcher_interval_seconds)
            except Exception:  # noqa: BLE001 - Redis/DB outage: rows stay in the outbox
                log.exception("dispatcher error; will retry")
                self.stop_event.wait(2.0)

    def stop(self) -> None:
        self.stop_event.set()


def main() -> None:
    logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"),
                        format="%(asctime)s %(levelname)s %(name)s %(message)s")
    init_db()
    d = Dispatcher()
    signal.signal(signal.SIGTERM, lambda *_: d.stop())
    signal.signal(signal.SIGINT, lambda *_: d.stop())
    d.run()


if __name__ == "__main__":
    main()
