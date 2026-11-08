from __future__ import annotations

import datetime as dt
import threading
from collections.abc import Callable
from typing import Any

import psycopg
import pytest

from pg_taskq import claim, complete, enqueue, fail, outbox, prune

from .conftest import T0, run_threads

Conn = psycopg.Connection[Any]
S = dt.timedelta(seconds=1)


def states(conn: Conn) -> dict[str, int]:
    rows = conn.execute("SELECT state, count(*) FROM taskq_jobs GROUP BY 1").fetchall()
    return {str(s): int(n) for s, n in rows}


def finish(conn: Conn, n: int, *, dead: bool = False, at: dt.datetime = T0) -> None:
    for _ in range(n):
        enqueue(conn, "q", max_attempts=1, now=at)
    for job in claim(conn, "q", "w", limit=n, now=at):
        if dead:
            fail(conn, job, "x", now=at)
        else:
            complete(conn, job, now=at)


def test_prune_deletes_only_old_finished_rows(conn: Conn) -> None:
    finish(conn, 3)  # done at T0
    finish(conn, 2, dead=True)  # dead at T0
    finish(conn, 4, at=T0 + 3600 * S)  # done an hour later
    enqueue(conn, "q", now=T0)  # queued, never pruned
    enqueue(conn, "q", now=T0)
    claim(conn, "q", "busy", now=T0)  # running, never pruned
    result = prune(conn, dt.timedelta(minutes=30), now=T0 + 3600 * S, batch=2)
    assert result.jobs == 5
    assert states(conn) == {"done": 4, "queued": 1, "running": 1}


def test_prune_by_state(conn: Conn) -> None:
    finish(conn, 2)
    finish(conn, 3, dead=True)
    assert prune(conn, S, states=["dead"], now=T0 + 60 * S).jobs == 3
    assert states(conn) == {"done": 2}


@pytest.mark.parametrize("bad", [[], ["queued"], ["done", "running"]])
def test_prune_never_touches_unfinished_states(conn: Conn, bad: list[str]) -> None:
    with pytest.raises(ValueError, match="subset"):
        prune(conn, S, states=bad)


def test_prune_validates_arguments(conn: Conn) -> None:
    with pytest.raises(ValueError, match="negative"):
        prune(conn, -S)
    with pytest.raises(ValueError, match="batch"):
        prune(conn, S, batch=0)
    with pytest.raises(ValueError, match="max_batches"):
        prune(conn, S, max_batches=0)


def test_prune_outbox_and_max_batches(conn: Conn) -> None:
    with conn.transaction():
        for n in range(7):
            outbox.add(conn, "t", {"n": n})
    outbox.relay_once(conn, lambda m: None, limit=5)  # 5 published, 2 pending
    later = dt.datetime.now(dt.UTC) + 60 * S
    assert prune(conn, S, now=later, batch=2, max_batches=1).outbox == 2
    assert prune(conn, S, now=later, batch=2).outbox == 3
    assert outbox.pending(conn) == 2  # unpublished messages are never pruned
    assert prune(conn, S, now=later, outbox=False).outbox == 0


def test_pruning_frees_the_idempotency_key(conn: Conn) -> None:
    first = enqueue(conn, "q", idempotency_key="k", now=T0)
    [job] = claim(conn, "q", "w", now=T0)
    complete(conn, job, now=T0)
    assert enqueue(conn, "q", idempotency_key="k", now=T0) == first
    prune(conn, S, now=T0 + 60 * S)
    assert enqueue(conn, "q", idempotency_key="k", now=T0) != first


def test_prune_runs_next_to_busy_workers(connect: Callable[[], Conn], conn: Conn) -> None:
    total = 600
    for n in range(total):
        enqueue(conn, "q", {"n": n}, notify=False)
    processed: list[int] = []
    pruned: list[int] = []
    workers_done = threading.Event()
    lock = threading.Lock()

    def worker(name: str) -> Callable[[], None]:
        def run() -> None:
            with connect() as c:
                while batch := claim(c, "q", name, limit=5):
                    for job in batch:
                        assert complete(c, job)
                        with lock:
                            processed.append(job.id)

        return run

    def pruner() -> None:
        with connect() as c:
            while not workers_done.is_set():
                pruned.append(prune(c, dt.timedelta(0), batch=25).jobs)
            pruned.append(prune(c, dt.timedelta(0), batch=25).jobs)

    def workers() -> None:
        try:
            run_threads(*[worker(f"w{i}") for i in range(4)])
        finally:
            workers_done.set()

    run_threads(workers, pruner)
    assert sorted(processed) == list(range(1, total + 1))  # each job exactly once
    # finished_at < now: a job finished in the very last instant may survive one more call.
    assert sum(pruned) + prune(conn, dt.timedelta(0)).jobs == total
    assert states(conn) == {}


def test_parallel_prunes_delete_each_row_once(connect: Callable[[], Conn], conn: Conn) -> None:
    finish(conn, 300)
    counts: list[int] = []

    def pruner() -> None:
        with connect() as c:
            counts.append(prune(c, S, batch=7, now=T0 + 60 * S).jobs)

    run_threads(*[pruner for _ in range(4)])
    assert sum(counts) == 300


def test_prune_skips_rows_locked_by_others(connect: Callable[[], Conn], conn: Conn) -> None:
    finish(conn, 5)
    holder, pruner = connect(), connect()
    pruner.execute("SET statement_timeout = '2s'")  # waiting on the lock would fail, not hang
    with holder.transaction():
        locked = holder.execute(
            "SELECT id FROM taskq_jobs ORDER BY id LIMIT 1 FOR UPDATE"
        ).fetchone()
        assert prune(pruner, S, now=T0 + 60 * S, batch=2).jobs == 4
    assert locked is not None
    assert prune(pruner, S, now=T0 + 60 * S).jobs == 1  # its turn once the lock is gone
