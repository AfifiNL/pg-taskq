"""Retention: delete finished jobs and published outbox messages after a while.

Deletes run in small batches, each its own short transaction (`LIMIT` + `FOR UPDATE SKIP
LOCKED`), so pruning never holds many locks or one long transaction, never waits for rows that
someone else has locked, and can run next to busy workers. Only `done` and `dead` jobs and
published messages are ever deleted; queued and running jobs are not eligible.

Call it on an idle connection: inside a caller's transaction every batch becomes a savepoint and
all locks are held until that transaction ends, which defeats the batching.

Pruning a job frees its idempotency key: enqueueing the same key again creates a new job.
"""

from __future__ import annotations

import datetime as dt
from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any

import psycopg

from pg_taskq.jobs import NOW, check_aware

FINAL_STATES = frozenset({"done", "dead"})


@dataclass(frozen=True, slots=True)
class Pruned:
    jobs: int
    outbox: int


def prune(
    conn: psycopg.Connection[Any],
    older_than: dt.timedelta,
    *,
    states: Iterable[str] = ("done", "dead"),
    outbox: bool = True,
    batch: int = 1000,
    max_batches: int | None = None,
    now: dt.datetime | None = None,
) -> Pruned:
    """Delete jobs in `states` that finished more than `older_than` ago, and (with `outbox`)
    messages published more than `older_than` ago. Returns how many rows were deleted.

    `max_batches` bounds the work per call (per table), for running it from a periodic task.
    """
    check_aware(now, "now")
    wanted = sorted(set(states))
    if not wanted or not set(wanted) <= FINAL_STATES:
        raise ValueError(f"states must be a non-empty subset of {sorted(FINAL_STATES)}")
    if older_than < dt.timedelta(0):
        raise ValueError("older_than must not be negative")
    if batch < 1:
        raise ValueError("batch must be >= 1")
    if max_batches is not None and max_batches < 1:
        raise ValueError("max_batches must be >= 1")
    params = {"states": wanted, "older_than": older_than, "batch": batch, "now": now}
    jobs = _in_batches(
        conn,
        f"""
        WITH victims AS (
            SELECT id FROM taskq_jobs
            WHERE state = ANY(%(states)s) AND finished_at < {NOW} - %(older_than)s
            ORDER BY finished_at, id
            LIMIT %(batch)s
            FOR UPDATE SKIP LOCKED
        )
        DELETE FROM taskq_jobs AS j USING victims WHERE j.id = victims.id
        """,  # noqa: S608  (NOW is a constant SQL fragment, all values are bound)
        params,
        batch,
        max_batches,
    )
    messages = 0
    if outbox:
        messages = _in_batches(
            conn,
            f"""
            WITH victims AS (
                SELECT id FROM taskq_outbox
                WHERE published_at IS NOT NULL AND published_at < {NOW} - %(older_than)s
                ORDER BY published_at, id
                LIMIT %(batch)s
                FOR UPDATE SKIP LOCKED
            )
            DELETE FROM taskq_outbox AS o USING victims WHERE o.id = victims.id
            """,  # noqa: S608
            params,
            batch,
            max_batches,
        )
    return Pruned(jobs=jobs, outbox=messages)


def _in_batches(
    conn: psycopg.Connection[Any],
    query: str,
    params: dict[str, Any],
    batch: int,
    max_batches: int | None,
) -> int:
    total = 0
    done = 0
    while max_batches is None or done < max_batches:
        with conn.transaction():
            deleted = conn.execute(query.encode(), params).rowcount
        total += deleted
        done += 1
        # Fewer than a full batch: nothing left that is not locked by someone else right now.
        if deleted < batch:
            break
    return total
