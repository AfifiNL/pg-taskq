from __future__ import annotations

import datetime as dt
import random
from collections.abc import Callable
from typing import Any

import psycopg
import pytest

from pg_taskq import (
    Backoff,
    FailOutcome,
    claim,
    complete,
    enqueue,
    fail,
    heartbeat,
    migrate,
    reap,
    schema_version,
    stats,
)
from pg_taskq.schema import LATEST

from .conftest import T0

Conn = psycopg.Connection[Any]
S = dt.timedelta(seconds=1)


def state(conn: Conn, job_id: int) -> dict[str, Any]:
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        row = cur.execute("SELECT * FROM taskq_jobs WHERE id = %s", (job_id,)).fetchone()
    assert row is not None
    return row


def test_migrate_is_idempotent(conn: Conn) -> None:
    assert migrate(conn) == []
    assert schema_version(conn) == LATEST


def test_enqueue_and_claim_roundtrip(conn: Conn) -> None:
    job_id = enqueue(conn, "mail", {"to": "a@example.com", "n": 1}, now=T0)
    [job] = claim(conn, "mail", "w1", now=T0)
    assert job.id == job_id
    assert job.payload == {"to": "a@example.com", "n": 1}
    assert job.attempts == 1
    assert job.locked_by == "w1"
    assert job.locked_until == T0 + 30 * S
    assert claim(conn, "mail", "w2", now=T0) == []  # nobody else gets it


def test_payload_defaults_to_empty_object(conn: Conn) -> None:
    enqueue(conn, "q", now=T0)
    assert claim(conn, "q", "w", now=T0)[0].payload == {}


def test_idempotency_key_is_per_queue(conn: Conn) -> None:
    a = enqueue(conn, "q", {"v": 1}, idempotency_key="order-1", now=T0)
    b = enqueue(conn, "q", {"v": 2}, idempotency_key="order-1", now=T0)
    c = enqueue(conn, "other", {"v": 3}, idempotency_key="order-1", now=T0)
    assert a == b != c
    assert state(conn, a)["payload"] == {"v": 1}  # the duplicate did not overwrite


def test_idempotent_enqueue_returns_finished_job_too(conn: Conn) -> None:
    a = enqueue(conn, "q", idempotency_key="k", now=T0)
    [job] = claim(conn, "q", "w", now=T0)
    assert complete(conn, job, now=T0)
    assert enqueue(conn, "q", idempotency_key="k", now=T0) == a


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        ({"queue": ""}, "queue"),
        ({"max_attempts": 0}, "max_attempts"),
        ({"run_at": T0, "delay": S}, "run_at or delay"),
    ],
)
def test_enqueue_validates(conn: Conn, kwargs: dict[str, Any], message: str) -> None:
    args: dict[str, Any] = {"queue": "q", **kwargs}
    queue = args.pop("queue")
    with pytest.raises(ValueError, match=message):
        enqueue(conn, queue, **args)


def test_claim_order_priority_then_age_then_id(conn: Conn) -> None:
    low_old = enqueue(conn, "q", run_at=T0 - 10 * S, now=T0)
    high_new = enqueue(conn, "q", priority=5, run_at=T0 - 1 * S, now=T0)
    low_new_1 = enqueue(conn, "q", run_at=T0 - 1 * S, now=T0)
    low_new_2 = enqueue(conn, "q", run_at=T0 - 1 * S, now=T0)
    got = [j.id for j in claim(conn, "q", "w", limit=10, now=T0)]
    assert got == [high_new, low_old, low_new_1, low_new_2]


def test_future_jobs_wait_until_due(conn: Conn) -> None:
    job_id = enqueue(conn, "q", delay=60 * S, now=T0)
    assert claim(conn, "q", "w", now=T0 + 59 * S) == []
    assert [j.id for j in claim(conn, "q", "w", now=T0 + 60 * S)] == [job_id]


def test_claim_respects_limit_and_queue(conn: Conn) -> None:
    for _ in range(5):
        enqueue(conn, "a", now=T0)
    enqueue(conn, "b", now=T0)
    assert len(claim(conn, "a", "w", limit=3, now=T0)) == 3
    assert len(claim(conn, "a", "w", limit=3, now=T0)) == 2
    assert len(claim(conn, "b", "w", limit=3, now=T0)) == 1


def test_heartbeat_extends_only_with_the_current_token(conn: Conn) -> None:
    enqueue(conn, "q", now=T0)
    [job] = claim(conn, "q", "w", lease=10 * S, now=T0)
    assert heartbeat(conn, job, lease=10 * S, now=T0 + 5 * S)
    assert state(conn, job.id)["locked_until"] == T0 + 15 * S
    # The lease expires, the reaper takes the job back and another worker claims it.
    assert reap(conn, now=T0 + 16 * S).requeued == [job.id]
    [again] = claim(conn, "q", "w2", now=T0 + 16 * S)
    assert again.id == job.id
    assert again.lease_token != job.lease_token
    assert not heartbeat(conn, job, now=T0 + 17 * S)
    assert not complete(conn, job, now=T0 + 17 * S)
    assert fail(conn, job, "late", now=T0 + 17 * S) is FailOutcome.LOST
    assert complete(conn, again, now=T0 + 17 * S)


def test_complete_once(conn: Conn) -> None:
    enqueue(conn, "q", now=T0)
    [job] = claim(conn, "q", "w", now=T0)
    assert complete(conn, job, now=T0 + S)
    assert not complete(conn, job, now=T0 + S)
    row = state(conn, job.id)
    assert row["state"] == "done"
    assert row["finished_at"] == T0 + S
    assert row["lease_token"] is None
    assert row["locked_by"] is None


def test_fail_retries_with_backoff_then_dies(conn: Conn) -> None:
    backoff = Backoff(base=10 * S, cap=60 * S, rng=random.Random(7))
    job_id = enqueue(conn, "q", max_attempts=2, now=T0)
    [job] = claim(conn, "q", "w", now=T0)
    assert fail(conn, job, "boom", backoff=backoff, now=T0) is FailOutcome.RETRY
    row = state(conn, job_id)
    assert row["state"] == "queued"
    assert row["last_error"] == "boom"
    assert T0 + 5 * S <= row["run_at"] <= T0 + 10 * S  # first retry: between d/2 and d
    assert claim(conn, "q", "w", now=T0 + 4 * S) == []
    [job] = claim(conn, "q", "w", now=row["run_at"])
    assert job.attempts == 2
    assert fail(conn, job, "boom again", backoff=backoff, now=row["run_at"]) is FailOutcome.DEAD
    row = state(conn, job_id)
    assert row["state"] == "dead"
    assert row["attempts"] == 2
    assert row["finished_at"] is not None
    assert claim(conn, "q", "w", now=T0 + 3600 * S) == []


def test_fail_without_retry_is_dead_at_once(conn: Conn) -> None:
    enqueue(conn, "q", max_attempts=5, now=T0)
    [job] = claim(conn, "q", "w", now=T0)
    assert fail(conn, job, "bad input", retry=False, now=T0) is FailOutcome.DEAD


def test_long_errors_are_truncated(conn: Conn) -> None:
    job_id = enqueue(conn, "q", now=T0)
    [job] = claim(conn, "q", "w", now=T0)
    fail(conn, job, "x" * 10_000, now=T0)
    assert len(state(conn, job_id)["last_error"]) == 2000


def test_reap_requeues_expired_and_kills_exhausted(conn: Conn) -> None:
    keep = enqueue(conn, "q", now=T0)
    retry = enqueue(conn, "q", max_attempts=3, now=T0)
    last = enqueue(conn, "q", max_attempts=1, now=T0)
    jobs = {j.id: j for j in claim(conn, "q", "w", limit=3, lease=10 * S, now=T0)}
    assert heartbeat(conn, jobs[keep], lease=60 * S, now=T0 + 5 * S)
    reaped = reap(conn, now=T0 + 11 * S)
    assert reaped.requeued == [retry]
    assert reaped.dead == [last]
    assert state(conn, keep)["state"] == "running"
    assert "stopped heartbeating" in state(conn, retry)["last_error"]
    assert reap(conn, now=T0 + 11 * S).requeued == []


def test_backoff_bounds_and_cap() -> None:
    b = Backoff(base=2 * S, cap=30 * S, rng=random.Random(1))
    for attempt in range(1, 40):
        full = min(30.0, 2.0 * 2 ** (attempt - 1))
        d = b.delay(attempt).total_seconds()
        assert full / 2 <= d <= full
    with pytest.raises(ValueError, match="attempts"):
        b.delay(0)
    with pytest.raises(ValueError, match="base"):
        Backoff(base=10 * S, cap=5 * S)


def test_skip_locked_does_not_block(connect: Callable[[], Conn]) -> None:
    a, b = connect(), connect()
    for _ in range(4):
        enqueue(a, "q", now=T0)
    b.execute("SET statement_timeout = '2s'")  # a blocking claim would fail, not hang
    with a.transaction():  # a's claim stays uncommitted, so its rows stay locked
        mine = claim(a, "q", "a", limit=2, now=T0)
        theirs = claim(b, "q", "b", limit=10, now=T0)
        assert {j.id for j in mine}.isdisjoint({j.id for j in theirs})
        assert len(theirs) == 2


def test_stats(conn: Conn) -> None:
    enqueue(conn, "a", now=T0)
    enqueue(conn, "a", now=T0)
    enqueue(conn, "b", now=T0)
    [job] = claim(conn, "a", "w", now=T0)
    complete(conn, job, now=T0)
    assert stats(conn) == {"a": {"done": 1, "queued": 1}, "b": {"queued": 1}}


def test_complete_inside_callers_transaction_rolls_back_together(conn: Conn) -> None:
    conn.execute("CREATE TEMP TABLE effects (job_id bigint)")
    job_id = enqueue(conn, "q", now=T0)
    [job] = claim(conn, "q", "w", now=T0)
    with pytest.raises(RuntimeError), conn.transaction():  # noqa: PT012 (rollback test)
        conn.execute("INSERT INTO effects VALUES (%s)", (job.id,))
        assert complete(conn, job, now=T0)
        raise RuntimeError("crash before commit")
    assert state(conn, job_id)["state"] == "running"
    assert conn.execute("SELECT count(*) FROM effects").fetchone() == (0,)
