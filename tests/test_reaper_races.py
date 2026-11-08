"""Reapers racing each other and racing heartbeats, on separate connections."""

from __future__ import annotations

import datetime as dt
from collections.abc import Callable
from typing import Any

import psycopg

from pg_taskq import claim, enqueue, heartbeat, reap

from .conftest import T0, run_threads

Conn = psycopg.Connection[Any]
S = dt.timedelta(seconds=1)


def expired_jobs(conn: Conn, n: int) -> list[Any]:
    for _ in range(n):
        enqueue(conn, "q", now=T0)
    return claim(conn, "q", "gone", limit=n, lease=10 * S, now=T0)


def test_an_open_reap_does_not_block_another(connect: Callable[[], Conn]) -> None:
    a, b = connect(), connect()
    expired_jobs(a, 10)
    b.execute("SET statement_timeout = '2s'")  # waiting on a's row locks would fail, not hang
    with a.transaction():  # a's reap stays uncommitted, so its rows stay locked
        first = reap(a, limit=4, now=T0 + 60 * S)
        second = reap(b, now=T0 + 60 * S)
    assert len(first.requeued) == 4
    assert len(second.requeued) == 6
    assert set(first.requeued).isdisjoint(second.requeued)


def test_parallel_reapers_take_each_job_once(connect: Callable[[], Conn]) -> None:
    jobs = expired_jobs(connect(), 300)
    results: list[list[int]] = []

    def reaper() -> None:
        with connect() as c:
            while got := reap(c, limit=7, now=T0 + 60 * S).requeued:
                results.append(got)

    run_threads(*[reaper for _ in range(4)])
    taken = [jid for batch in results for jid in batch]
    assert sorted(taken) == sorted(j.id for j in jobs)


def test_heartbeat_racing_the_reaper_has_one_winner(connect: Callable[[], Conn]) -> None:
    jobs = expired_jobs(connect(), 200)
    beats: dict[int, bool] = {}
    reaped: list[int] = []

    def beater() -> None:  # the workers' clocks say the leases are still fine
        with connect() as c:
            for job in jobs:
                beats[job.id] = heartbeat(c, job, lease=30 * S, now=T0 + 9 * S)

    def reaper() -> None:  # the reaper's clock says they expired
        with connect() as c:
            while got := reap(c, limit=5, now=T0 + 11 * S).requeued:
                reaped.extend(got)

    run_threads(beater, reaper)
    conn = connect()
    for job in jobs:
        row = conn.execute("SELECT state FROM taskq_jobs WHERE id = %s", (job.id,)).fetchone()
        assert row is not None
        # Exactly one side won: a successful heartbeat keeps the job running and unreaped;
        # otherwise the reaper took it and the heartbeat was refused.
        assert beats[job.id] != (job.id in reaped)
        assert (row[0] == "running") == beats[job.id]
