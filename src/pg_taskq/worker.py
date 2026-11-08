"""A small worker loop: claim one job, keep its lease alive while the handler runs, complete it.

    def handle(job: Job, conn: psycopg.Connection) -> None:
        conn.execute("INSERT INTO invoices ...", ...)

    Worker(connect, "emails", handle, worker_id="host-1:pid").run()

`connect` is a zero-argument callable returning a new connection. The worker keeps up to three:
one for the loop and the handler, one for heartbeats (a connection runs one statement at a time,
and a heartbeat must not wait behind a long handler query), and with `listen=True` one that only
LISTENs for wake-ups. All are re-opened with backoff when they break (a restarted server, a
killed backend, a network blip); `run()` logs and carries on.

Wake-ups (`listen=True`) pair with `enqueue(..., notify=True)`; both are off by default (see
docs/DESIGN.md for the costs). The LISTEN connection must reach PostgreSQL directly or through a
session-mode pool: under transaction pooling the LISTEN lands on whichever server connection ran
that statement, and the notifications are lost or delivered to other clients.

Two modes, chosen with `transactional`:

* `transactional=True` (default): the handler and `complete()` run in ONE transaction. If the
  lease was lost meanwhile, `complete()` matches no row and the handler's writes roll back, so a
  job's database effects happen once even if the handler ran twice. The price: that transaction
  stays open for the whole job, which holds back vacuum and is cut off by
  `idle_in_transaction_session_timeout` if the handler spends long outside the database. Use it
  for jobs that are short or mostly database work.
* `transactional=False`: the handler gets an autocommit connection and manages its own (short)
  transactions; `complete()` runs afterwards in a transaction of its own, still fenced by the
  lease token. A lost lease is detected and counted, but writes the handler already committed
  stay. Use it for long jobs, and make their effects idempotent.

Effects outside the database (e-mails, HTTP calls) are at-least-once in both modes.
"""

from __future__ import annotations

import contextlib
import datetime as dt
import logging
import os
import socket
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

import psycopg
from psycopg import sql

from pg_taskq import jobs
from pg_taskq.jobs import Backoff, Job, check_name, describe_error

log = logging.getLogger("pg_taskq.worker")

Handler = Callable[[Job, psycopg.Connection[Any]], None]
Connect = Callable[[], psycopg.Connection[Any]]


class LeaseLostError(RuntimeError):
    """The worker no longer holds the job's lease."""


@dataclass
class WorkerStats:
    completed: int = 0
    retried: int = 0
    dead: int = 0
    lost: int = 0
    reconnects: int = 0
    wakeups: int = 0  # idle waits ended early by a NOTIFY
    listen_checks: int = 0  # periodic re-LISTENs that proved the LISTEN connection alive


# TCP keepalive and user-timeout settings for the LISTEN connection, which may sit idle for a
# long time: a dead peer (a dropped NAT entry, a crashed host) is detected within about a minute
# instead of the kernel default of hours, and the next read or write fails instead of hanging.
KEEPALIVE_IDLE_S = 30
KEEPALIVE_INTERVAL_S = 10
KEEPALIVE_COUNT = 3
USER_TIMEOUT_MS = 60_000


def harden_socket(conn: Any) -> bool:
    """Turn on aggressive TCP keepalives for a connection's socket. Returns False (and changes
    nothing) for Unix-domain sockets or platforms without these options."""
    try:
        sock = socket.socket(fileno=os.dup(conn.pgconn.socket))
    except (OSError, AttributeError, psycopg.Error):
        return False
    with sock:
        if sock.family not in (socket.AF_INET, socket.AF_INET6):
            return False
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
        for name, value in (
            ("TCP_KEEPIDLE", KEEPALIVE_IDLE_S),
            ("TCP_KEEPINTVL", KEEPALIVE_INTERVAL_S),
            ("TCP_KEEPCNT", KEEPALIVE_COUNT),
            ("TCP_USER_TIMEOUT", USER_TIMEOUT_MS),
        ):
            option = getattr(socket, name, None)
            if option is not None:
                sock.setsockopt(socket.IPPROTO_TCP, option, value)
    return True


class _Link:
    """One connection that is re-opened on demand after it breaks."""

    def __init__(
        self,
        connect: Connect,
        stats: WorkerStats,
        on_connect: Callable[[psycopg.Connection[Any]], None] | None = None,
    ) -> None:
        self._connect = connect
        self._stats = stats
        self._on_connect = on_connect
        self._conn: psycopg.Connection[Any] | None = None
        self._opened = False

    def get(self) -> psycopg.Connection[Any]:
        if self._conn is None or self._conn.closed or self._conn.broken:
            self.reset()
            conn = self._connect()
            try:
                conn.autocommit = True
                if self._on_connect is not None:
                    self._on_connect(conn)
            except BaseException:
                with contextlib.suppress(psycopg.Error):  # don't leak a half-set-up connection
                    conn.close()
                raise
            self._conn = conn
            if self._opened:
                self._stats.reconnects += 1
            self._opened = True
        return self._conn

    @property
    def connected(self) -> bool:
        return self._conn is not None and not self._conn.closed and not self._conn.broken

    def reset(self) -> None:
        if self._conn is not None:
            with contextlib.suppress(psycopg.Error):  # closing a broken connection
                self._conn.close()
            self._conn = None


class _Heartbeat(threading.Thread):
    def __init__(self, link: _Link, job: Job, lease: dt.timedelta, every: float) -> None:
        super().__init__(name=f"taskq-heartbeat-{job.id}", daemon=True)
        self.link, self.job, self.lease, self.every = link, job, lease, every
        self.done = threading.Event()
        self.lost = threading.Event()

    def run(self) -> None:
        while not self.done.wait(self.every):
            try:
                ok = jobs.heartbeat(self.link.get(), self.job, lease=self.lease)
            except psycopg.Error as exc:
                # A missed beat is not fatal: the lease has slack (beats are every lease/3 by
                # default). Drop the connection; the next beat opens a new one.
                log.warning("heartbeat for job %s failed: %s", self.job.id, describe_error(exc))
                self.link.reset()
                continue
            if not ok:
                self.lost.set()
                return


class Worker:
    def __init__(
        self,
        connect: Connect,
        queue: str,
        handler: Handler,
        *,
        worker_id: str,
        lease: dt.timedelta = dt.timedelta(seconds=30),
        heartbeat_every: float | None = None,
        poll_interval: float = 1.0,
        reap_every: float | None = 30.0,
        backoff: Backoff | None = None,
        transactional: bool = True,
        listen: bool = False,
        listen_check_every: float = 60.0,
        reconnect_backoff: Backoff | None = None,
    ) -> None:
        check_name(queue, "queue")
        check_name(worker_id, "worker_id")
        if heartbeat_every is None:
            heartbeat_every = lease.total_seconds() / 3
        if heartbeat_every <= 0 or heartbeat_every >= lease.total_seconds():
            raise ValueError("heartbeat_every must be positive and shorter than the lease")
        self.queue, self.handler, self.worker_id = queue, handler, worker_id
        self.lease, self.heartbeat_every = lease, heartbeat_every
        self.poll_interval = poll_interval
        self.reap_every = reap_every
        self.backoff = backoff or Backoff()
        self.transactional = transactional
        self.listen = listen
        self.listen_check_every = listen_check_every
        self.channel: str | None = None  # learned from the database on the first LISTEN
        self._last_listen_check = 0.0
        self._listen_failing = False
        self.reconnect_backoff = reconnect_backoff or Backoff(
            base=dt.timedelta(milliseconds=200), cap=dt.timedelta(seconds=10)
        )
        self.stats = WorkerStats()
        self._main = _Link(connect, self.stats)
        self._beat_link = _Link(connect, self.stats)
        # Wake-ups: a third connection that only LISTENs on the queue's channel.
        self._listen_link = _Link(connect, self.stats, on_connect=self._listen_on)
        self._stop = threading.Event()

    def _listen_on(self, conn: psycopg.Connection[Any]) -> None:
        # The channel depends on the schema the tables are in; ask the database, which computes
        # it exactly as enqueue() does.
        row = conn.execute(
            f"SELECT {jobs.CHANNEL_SQL.format(queue='%s')}",
            (self.queue,),
        ).fetchone()
        if row is None:  # pragma: no cover
            raise psycopg.InterfaceError("could not compute the queue's channel")
        self.channel = str(row[0])
        harden_socket(conn)
        conn.execute(sql.SQL("LISTEN {}").format(sql.Identifier(self.channel)))
        self._last_listen_check = time.monotonic()

    def stop(self) -> None:
        """Finish the current job (if any), then return from `run`."""
        self._stop.set()

    def run(self, *, max_jobs: int | None = None, until_empty: bool = False) -> WorkerStats:
        handled = 0
        failures = 0  # consecutive connection failures, for the reconnect backoff
        last_reap = float("-inf")
        try:
            while not self._stop.is_set():
                if max_jobs is not None and handled >= max_jobs:
                    break
                if self.listen:
                    self._ensure_listening()
                try:
                    conn = self._main.get()
                    if self.reap_every is not None and time.monotonic() - last_reap >= (
                        self.reap_every
                    ):
                        # Any worker may reap; SKIP LOCKED keeps concurrent reapers apart.
                        reaped = jobs.reap(conn)
                        if reaped.requeued or reaped.dead:
                            log.info("reaped: requeued=%s dead=%s", reaped.requeued, reaped.dead)
                        last_reap = time.monotonic()
                    claimed = jobs.claim(
                        conn, self.queue, self.worker_id, limit=1, lease=self.lease
                    )
                except psycopg.OperationalError as exc:
                    failures += 1
                    wait = self.reconnect_backoff.delay(failures).total_seconds()
                    log.warning(
                        "database unavailable (%s); retrying in %.1f s", describe_error(exc), wait
                    )
                    self._main.reset()
                    self._stop.wait(wait)
                    continue
                failures = 0
                if not claimed:
                    if until_empty:
                        break
                    self._idle()
                    continue
                self._process(claimed[0])
                self._drain_listen()
                handled += 1
        finally:
            self._main.reset()
            self._beat_link.reset()
            self._listen_link.reset()
        return self.stats

    def _ensure_listening(self) -> None:
        """LISTEN before claiming: a job enqueued after an empty claim then leaves a notification
        waiting, and the idle wait returns at once. Best effort: if LISTEN fails, the worker
        keeps claiming and falls back to polling (notifications are only an optimisation)."""
        try:
            self._listen_link.get()
            self._listen_failing = False
        except psycopg.Error as exc:
            if not self._listen_failing:  # log once per outage, not once per poll
                log.warning("cannot LISTEN (%s); polling until it works", describe_error(exc))
            self._listen_failing = True
            self._listen_link.reset()

    def _idle(self) -> None:
        """Wait up to `poll_interval` for a NOTIFY on the queue's channel (or for stop()).

        A notification only shortens the wait: if one is lost (the LISTEN connection broke, or a
        job was enqueued without notify), the next poll finds the job anyway. A broken LISTEN
        connection is re-opened (and re-LISTENed) at once, not after the poll interval.
        """
        deadline = time.monotonic() + self.poll_interval
        failures = 0
        while not self._stop.is_set():
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return
            if not self.listen:
                self._stop.wait(remaining)
                return
            try:
                conn = self._listen_link.get()
                self._check_listen(conn)
                # Short slices keep stop() responsive. The generator holds the connection's
                # lock while it is suspended, so close it before touching the connection again.
                pending = conn.notifies(timeout=min(remaining, 0.25), stop_after=1)
                try:
                    woke = next(pending, None) is not None
                finally:
                    pending.close()
                if woke:
                    self._drain(conn)  # several commits may have notified; one claim serves all
                    self.stats.wakeups += 1
                    return
                failures = 0
            except psycopg.Error as exc:
                failures += 1
                wait = min(remaining, self.reconnect_backoff.delay(failures).total_seconds())
                log.warning(
                    "LISTEN connection failed (%s); retrying in %.1f s", describe_error(exc), wait
                )
                self._listen_link.reset()
                self._stop.wait(wait)

    def _check_listen(self, conn: psycopg.Connection[Any]) -> None:
        """Every `listen_check_every` seconds, re-issue the LISTEN (a no-op when already
        listening). The round trip proves the connection is alive; with the keepalive settings
        a half-open TCP connection fails here instead of waiting silently forever."""
        if self.channel is None or (
            time.monotonic() - self._last_listen_check < self.listen_check_every
        ):
            return
        conn.execute(sql.SQL("LISTEN {}").format(sql.Identifier(self.channel)))
        self._last_listen_check = time.monotonic()
        self.stats.listen_checks += 1

    @staticmethod
    def _drain(conn: psycopg.Connection[Any]) -> None:
        for _ in conn.notifies(timeout=0):
            pass

    def _drain_listen(self) -> None:
        """Read pending notifications after each job, so a busy worker's LISTEN connection never
        falls far behind (a listener that stops reading holds back PostgreSQL's notification
        queue for everyone). Best effort."""
        if not (self.listen and self._listen_link.connected):
            return
        try:
            self._drain(self._listen_link.get())
        except psycopg.Error:
            self._listen_link.reset()

    def _process(self, job: Job) -> None:
        beat = _Heartbeat(self._beat_link, job, self.lease, self.heartbeat_every)
        beat.start()
        try:
            conn = self._main.get()
            if self.transactional:
                with conn.transaction():
                    self.handler(job, conn)
                    if beat.lost.is_set() or not jobs.complete(conn, job):
                        raise LeaseLostError(f"job {job.id}: lease lost; rolling back its writes")
            else:
                self.handler(job, conn)
                if beat.lost.is_set() or not jobs.complete(conn, job):
                    raise LeaseLostError(f"job {job.id}: lease lost after the handler ran")
            self.stats.completed += 1
        except LeaseLostError as exc:
            log.warning("%s", exc)
            self.stats.lost += 1
        except Exception as exc:  # the handler's failure is recorded on the job
            self._record_failure(job, exc)
        finally:
            beat.done.set()
            # The heartbeat thread exits within one statement; never hang the worker on it.
            beat.join(timeout=self.heartbeat_every + 5.0)

    def _record_failure(self, job: Job, exc: Exception) -> None:
        error = describe_error(exc)
        try:
            outcome = jobs.fail(self._main.get(), job, error, backoff=self.backoff)
        except psycopg.Error as db_exc:
            # The connection broke (possibly what made the handler fail). Leave the job to the
            # reaper: its lease expires and it is retried, with this attempt counted.
            log.warning(
                "job %s failed (%s) and the failure could not be recorded (%s); "
                "the reaper will take it back when its lease expires",
                job.id,
                error,
                describe_error(db_exc),
            )
            self._main.reset()
            self.stats.lost += 1
            return
        if outcome is jobs.FailOutcome.RETRY:
            self.stats.retried += 1
        elif outcome is jobs.FailOutcome.DEAD:
            self.stats.dead += 1
        else:
            self.stats.lost += 1
