"""Jobs: enqueue, claim with a lease, heartbeat, complete, fail with backoff, reap stale leases.

Ownership model
---------------
A claim gives the worker a *lease*: a fresh random `lease_token` plus `locked_until`. Every
later operation on the job (heartbeat, complete, fail) must present that token and succeeds only
while the row still carries it. The reaper is the only thing that takes a lease away: it clears
the token of a job whose `locked_until` has passed and requeues it (or marks it dead when its
attempts are used up). A worker that stopped heartbeating therefore cannot complete or fail a
job that someone else has since claimed: its token no longer matches, and the call returns
False / LOST instead of touching the row.

Every function runs in `conn.transaction()`: on an idle connection that is its own committed
transaction; inside a caller's transaction it is a savepoint and commits with the caller. That
is what lets a handler write its results and mark the job done atomically.

Time: by default the database clock (`statement_timestamp()`) is the only clock, so workers on
different hosts never compare their own clocks. Tests pass `now=` to control time.
"""

from __future__ import annotations

import datetime as dt
import random
from collections.abc import Mapping
from dataclasses import dataclass, field, fields
from enum import StrEnum
from typing import Any, Final
from uuid import UUID

import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

NOW: Final = "coalesce(%(now)s::timestamptz, statement_timestamp())"
MAX_ERROR_CHARS: Final = 2000


class FailOutcome(StrEnum):
    RETRY = "retry"  # requeued with a backoff delay
    DEAD = "dead"  # attempts used up, or retry=False
    LOST = "lost"  # the caller no longer holds the lease; nothing changed


@dataclass(frozen=True, slots=True)
class Job:
    id: int
    queue: str
    payload: Any
    priority: int
    attempts: int
    max_attempts: int
    lease_token: UUID
    locked_by: str
    locked_until: dt.datetime
    run_at: dt.datetime
    created_at: dt.datetime

    @classmethod
    def from_row(cls, row: Mapping[str, Any]) -> Job:
        return cls(**{f.name: row[f.name] for f in fields(cls)})


@dataclass(slots=True)
class Backoff:
    """Exponential backoff with "equal jitter": half the delay is fixed, half is random.

    Retry n (n = attempts so far, from 1) waits between d/2 and d, where
    d = min(cap, base * 2 ** (n - 1)). The fixed half guarantees a minimum spacing; the random
    half spreads retries of jobs that failed together (for example during an outage).
    """

    base: dt.timedelta = dt.timedelta(seconds=5)
    cap: dt.timedelta = dt.timedelta(minutes=15)
    rng: random.Random = field(default_factory=random.Random)

    def __post_init__(self) -> None:
        if self.base <= dt.timedelta(0) or self.cap < self.base:
            raise ValueError("backoff needs 0 < base <= cap")

    def delay(self, attempts: int) -> dt.timedelta:
        if attempts < 1:
            raise ValueError("attempts must be >= 1")
        # In float seconds: base * 2**n overflows timedelta long before it overflows a float.
        exponent = min(attempts - 1, 1000)
        full = min(self.cap.total_seconds(), self.base.total_seconds() * 2.0**exponent)
        half = full / 2
        # Not cryptographic: jitter only needs to spread retries out.
        return dt.timedelta(seconds=half + half * self.rng.random())


def enqueue(
    conn: psycopg.Connection[Any],
    queue: str,
    payload: Any = None,
    *,
    run_at: dt.datetime | None = None,
    delay: dt.timedelta | None = None,
    priority: int = 0,
    max_attempts: int = 5,
    idempotency_key: str | None = None,
    now: dt.datetime | None = None,
) -> int:
    """Add a job and return its id. With an `idempotency_key` that already exists in this queue,
    nothing is inserted and the existing job's id is returned (whatever its state)."""
    if not queue:
        raise ValueError("queue must be a non-empty string")
    if max_attempts < 1:
        raise ValueError("max_attempts must be >= 1")
    if run_at is not None and delay is not None:
        raise ValueError("pass run_at or delay, not both")
    params = {
        "queue": queue,
        "payload": Jsonb({} if payload is None else payload),
        "run_at": run_at,
        "delay": delay or dt.timedelta(0),
        "priority": priority,
        "max_attempts": max_attempts,
        "key": idempotency_key,
        "now": now,
    }
    with conn.transaction():
        row = conn.execute(
            f"""
            INSERT INTO taskq_jobs (queue, payload, priority, max_attempts, idempotency_key,
                                    run_at, created_at, updated_at)
            VALUES (%(queue)s, %(payload)s, %(priority)s, %(max_attempts)s, %(key)s,
                    coalesce(%(run_at)s::timestamptz, {NOW} + %(delay)s), {NOW}, {NOW})
            ON CONFLICT (queue, idempotency_key) WHERE idempotency_key IS NOT NULL DO NOTHING
            RETURNING id
            """,  # noqa: S608  (NOW is a constant SQL fragment, all values are bound)
            params,
        ).fetchone()
        if row is None:
            row = conn.execute(
                "SELECT id FROM taskq_jobs WHERE queue = %s AND idempotency_key = %s",
                (queue, idempotency_key),
            ).fetchone()
        if row is None:  # pragma: no cover - the conflicting row cannot vanish in between
            raise RuntimeError("idempotent enqueue found neither a new nor an existing row")
        return int(row[0])


def claim(
    conn: psycopg.Connection[Any],
    queue: str,
    worker_id: str,
    *,
    limit: int = 1,
    lease: dt.timedelta = dt.timedelta(seconds=30),
    now: dt.datetime | None = None,
) -> list[Job]:
    """Atomically take up to `limit` due jobs from `queue` (highest priority first, then oldest).

    `FOR UPDATE SKIP LOCKED` makes concurrent claimers skip rows another claimer is taking right
    now instead of waiting for them, so N workers never block each other and never get the same
    row. Each claim counts as an attempt.
    """
    if limit < 1:
        raise ValueError("limit must be >= 1")
    if lease <= dt.timedelta(0):
        raise ValueError("lease must be positive")
    with conn.transaction(), conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            f"""
            WITH picked AS (
                SELECT id FROM taskq_jobs
                WHERE queue = %(queue)s AND state = 'queued' AND run_at <= {NOW}
                  AND attempts < max_attempts
                ORDER BY priority DESC, run_at, id
                LIMIT %(limit)s
                FOR UPDATE SKIP LOCKED
            )
            UPDATE taskq_jobs AS j
            SET state = 'running', attempts = j.attempts + 1, lease_token = gen_random_uuid(),
                locked_by = %(worker)s, locked_until = {NOW} + %(lease)s, updated_at = {NOW}
            FROM picked WHERE j.id = picked.id
            RETURNING j.*
            """,  # noqa: S608
            {"queue": queue, "limit": limit, "worker": worker_id, "lease": lease, "now": now},
        )
        jobs = [Job.from_row(r) for r in cur.fetchall()]
    return sorted(jobs, key=lambda j: (-j.priority, j.run_at, j.id))


def heartbeat(
    conn: psycopg.Connection[Any],
    job: Job,
    *,
    lease: dt.timedelta = dt.timedelta(seconds=30),
    now: dt.datetime | None = None,
) -> bool:
    """Extend the lease to now + `lease`. False means the lease was lost: stop working on it."""
    with conn.transaction():
        cur = conn.execute(
            f"""
            UPDATE taskq_jobs SET locked_until = {NOW} + %(lease)s, updated_at = {NOW}
            WHERE id = %(id)s AND state = 'running' AND lease_token = %(token)s
            """,  # noqa: S608
            {"id": job.id, "token": job.lease_token, "lease": lease, "now": now},
        )
        return cur.rowcount == 1


def complete(conn: psycopg.Connection[Any], job: Job, *, now: dt.datetime | None = None) -> bool:
    """Mark the job done. False (and no change) when the caller no longer holds the lease."""
    with conn.transaction():
        cur = conn.execute(
            f"""
            UPDATE taskq_jobs
            SET state = 'done', lease_token = NULL, locked_by = NULL, locked_until = NULL,
                last_error = NULL, finished_at = {NOW}, updated_at = {NOW}
            WHERE id = %(id)s AND state = 'running' AND lease_token = %(token)s
            """,  # noqa: S608
            {"id": job.id, "token": job.lease_token, "now": now},
        )
        return cur.rowcount == 1


def fail(
    conn: psycopg.Connection[Any],
    job: Job,
    error: str,
    *,
    retry: bool = True,
    backoff: Backoff | None = None,
    now: dt.datetime | None = None,
) -> FailOutcome:
    """Record a failed attempt: requeue after a backoff delay, or mark dead when the attempts
    are used up (or `retry=False`). Returns LOST, changing nothing, without a valid lease."""
    delay = (backoff or Backoff()).delay(max(job.attempts, 1))
    with conn.transaction():
        row = conn.execute(
            f"""
            UPDATE taskq_jobs
            SET state = CASE WHEN %(retry)s AND attempts < max_attempts
                             THEN 'queued' ELSE 'dead' END,
                run_at = CASE WHEN %(retry)s AND attempts < max_attempts
                              THEN {NOW} + %(delay)s ELSE run_at END,
                finished_at = CASE WHEN %(retry)s AND attempts < max_attempts
                                   THEN NULL ELSE {NOW} END,
                lease_token = NULL, locked_by = NULL, locked_until = NULL,
                last_error = %(error)s, updated_at = {NOW}
            WHERE id = %(id)s AND state = 'running' AND lease_token = %(token)s
            RETURNING state
            """,  # noqa: S608
            {
                "id": job.id,
                "token": job.lease_token,
                "retry": retry,
                "delay": delay,
                "error": error[:MAX_ERROR_CHARS],
                "now": now,
            },
        ).fetchone()
    if row is None:
        return FailOutcome.LOST
    return FailOutcome.RETRY if row[0] == "queued" else FailOutcome.DEAD


@dataclass(frozen=True, slots=True)
class Reaped:
    requeued: list[int]
    dead: list[int]


def reap(
    conn: psycopg.Connection[Any],
    *,
    limit: int = 100,
    now: dt.datetime | None = None,
) -> Reaped:
    """Take back leases that expired (the worker crashed, hung or lost its connection).

    A reaped job is requeued at once, or marked dead when the crashed attempt was its last.
    The attempt is not refunded: a job that crashes its worker every time must die eventually.
    """
    with conn.transaction():
        rows = conn.execute(
            f"""
            WITH stale AS (
                SELECT id FROM taskq_jobs
                WHERE state = 'running' AND locked_until < {NOW}
                ORDER BY locked_until, id
                LIMIT %(limit)s
                FOR UPDATE SKIP LOCKED
            )
            UPDATE taskq_jobs AS j
            SET state = CASE WHEN j.attempts < j.max_attempts THEN 'queued' ELSE 'dead' END,
                run_at = {NOW},
                finished_at = CASE WHEN j.attempts < j.max_attempts THEN NULL ELSE {NOW} END,
                last_error = 'lease expired (worker ' || j.locked_by || ' stopped heartbeating)',
                lease_token = NULL, locked_by = NULL, locked_until = NULL, updated_at = {NOW}
            FROM stale WHERE j.id = stale.id
            RETURNING j.id, j.state
            """,  # noqa: S608
            {"limit": limit, "now": now},
        ).fetchall()
    return Reaped(
        requeued=sorted(int(r[0]) for r in rows if r[1] == "queued"),
        dead=sorted(int(r[0]) for r in rows if r[1] == "dead"),
    )


def stats(conn: psycopg.Connection[Any]) -> dict[str, dict[str, int]]:
    """Job counts per queue and state."""
    out: dict[str, dict[str, int]] = {}
    for queue, state, n in conn.execute(
        "SELECT queue, state, count(*) FROM taskq_jobs GROUP BY queue, state ORDER BY 1, 2"
    ).fetchall():
        out.setdefault(str(queue), {})[str(state)] = int(n)
    return out
