"""A small worker loop: claim one job, keep its lease alive while the handler runs, then
complete it in the same transaction as the handler's own writes.

    def handle(job: Job, conn: psycopg.Connection) -> None:
        conn.execute("INSERT INTO invoices ...", ...)   # commits only if the job completes

    Worker(connect, "emails", handle, worker_id="host-1:pid").run()

`connect` is a zero-argument callable returning a new connection; the worker opens two (one
for the loop and the handler, one for heartbeats, because a connection runs one statement at a
time and the heartbeat must not wait behind a long handler query).

If the lease is lost while the handler runs (heartbeats failed for longer than the lease and the
reaper requeued the job), `complete` finds a foreign token and the worker rolls the handler's
transaction back, so the job's database effects happen once even though the handler ran twice.
Effects outside the database (e-mails, HTTP calls) are not covered; make them idempotent.
"""

from __future__ import annotations

import datetime as dt
import logging
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

import psycopg

from pg_taskq import jobs
from pg_taskq.jobs import Backoff, Job

log = logging.getLogger("pg_taskq.worker")

Handler = Callable[[Job, psycopg.Connection[Any]], None]


class LeaseLostError(RuntimeError):
    """The worker no longer holds the job's lease; its transaction is rolled back."""


@dataclass
class WorkerStats:
    completed: int = 0
    retried: int = 0
    dead: int = 0
    lost: int = 0


class _Heartbeat(threading.Thread):
    def __init__(
        self, conn: psycopg.Connection[Any], job: Job, lease: dt.timedelta, every: float
    ) -> None:
        super().__init__(name=f"taskq-heartbeat-{job.id}", daemon=True)
        self.conn, self.job, self.lease, self.every = conn, job, lease, every
        self.done = threading.Event()
        self.lost = threading.Event()

    def run(self) -> None:
        while not self.done.wait(self.every):
            try:
                ok = jobs.heartbeat(self.conn, self.job, lease=self.lease)
            except psycopg.Error as exc:  # a missed beat is not fatal; the lease has slack
                log.warning("heartbeat for job %s failed: %s", self.job.id, type(exc).__name__)
                continue
            if not ok:
                self.lost.set()
                return


class Worker:
    def __init__(
        self,
        connect: Callable[[], psycopg.Connection[Any]],
        queue: str,
        handler: Handler,
        *,
        worker_id: str,
        lease: dt.timedelta = dt.timedelta(seconds=30),
        heartbeat_every: float | None = None,
        poll_interval: float = 1.0,
        reap_every: float | None = 30.0,
        backoff: Backoff | None = None,
    ) -> None:
        if heartbeat_every is None:
            heartbeat_every = lease.total_seconds() / 3
        if heartbeat_every <= 0 or heartbeat_every >= lease.total_seconds():
            raise ValueError("heartbeat_every must be positive and shorter than the lease")
        self.connect, self.queue, self.handler = connect, queue, handler
        self.worker_id = worker_id
        self.lease, self.heartbeat_every = lease, heartbeat_every
        self.poll_interval = poll_interval
        self.reap_every = reap_every
        self.backoff = backoff or Backoff()
        self.stats = WorkerStats()
        self._stop = threading.Event()

    def stop(self) -> None:
        """Finish the current job (if any), then return from `run`."""
        self._stop.set()

    def run(self, *, max_jobs: int | None = None, until_empty: bool = False) -> WorkerStats:
        with self.connect() as conn, self.connect() as hb_conn:
            conn.autocommit = True
            hb_conn.autocommit = True
            handled = 0
            last_reap = float("-inf")
            while not self._stop.is_set():
                if max_jobs is not None and handled >= max_jobs:
                    break
                if self.reap_every is not None and time.monotonic() - last_reap >= self.reap_every:
                    # Any worker may reap; SKIP LOCKED keeps concurrent reapers apart.
                    reaped = jobs.reap(conn)
                    if reaped.requeued or reaped.dead:
                        log.info("reaped: requeued=%s dead=%s", reaped.requeued, reaped.dead)
                    last_reap = time.monotonic()
                claimed = jobs.claim(conn, self.queue, self.worker_id, limit=1, lease=self.lease)
                if not claimed:
                    if until_empty:
                        break
                    self._stop.wait(self.poll_interval)
                    continue
                self._process(conn, hb_conn, claimed[0])
                handled += 1
        return self.stats

    def _process(
        self, conn: psycopg.Connection[Any], hb_conn: psycopg.Connection[Any], job: Job
    ) -> None:
        beat = _Heartbeat(hb_conn, job, self.lease, self.heartbeat_every)
        beat.start()
        try:
            with conn.transaction():
                self.handler(job, conn)
                if beat.lost.is_set() or not jobs.complete(conn, job):
                    raise LeaseLostError(f"job {job.id}: lease lost; rolling back its effects")
            self.stats.completed += 1
        except LeaseLostError as exc:
            log.warning("%s", exc)
            self.stats.lost += 1
        except Exception as exc:  # the handler's failure is recorded on the job
            outcome = jobs.fail(conn, job, f"{type(exc).__name__}: {exc}", backoff=self.backoff)
            if outcome is jobs.FailOutcome.RETRY:
                self.stats.retried += 1
            elif outcome is jobs.FailOutcome.DEAD:
                self.stats.dead += 1
            else:
                self.stats.lost += 1
        finally:
            beat.done.set()
            beat.join()
