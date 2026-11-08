"""LISTEN/NOTIFY wake-ups, and the polling fallback when a notification never arrives."""

from __future__ import annotations

import datetime as dt
import threading
import time
from collections.abc import Callable
from typing import Any

import psycopg
import pytest
from psycopg import sql

from pg_taskq import Job, Worker, channel_for, claim, enqueue, reap

from .conftest import T0, run_threads

Conn = psycopg.Connection[Any]
S = dt.timedelta(seconds=1)


def listener(connect: Callable[[], Conn], queue: str) -> Conn:
    c = connect()
    c.execute(sql.SQL("LISTEN {}").format(sql.Identifier(channel_for(queue))))
    return c


def received(c: Conn, timeout: float = 0.5) -> int:
    return sum(1 for _ in c.notifies(timeout=timeout))


@pytest.mark.parametrize("queue", ["q", "Queue With Spaces", 'quote"s', "lång-ünïcode", "x" * 300])
def test_channel_names_are_safe_identifiers(queue: str) -> None:
    channel = channel_for(queue)
    assert channel.startswith("taskq_")
    assert len(channel) <= 63  # PostgreSQL's identifier limit
    assert channel.replace("_", "").isalnum()
    assert channel == channel.lower()
    assert channel_for(queue) == channel != channel_for(queue + "!")


def test_enqueue_notifies_on_commit_only(connect: Callable[[], Conn], conn: Conn) -> None:
    ear = listener(connect, "q")
    with pytest.raises(RuntimeError), conn.transaction():  # noqa: PT012 (rollback test)
        enqueue(conn, "q")
        raise RuntimeError("rolled back")
    assert received(ear) == 0
    with conn.transaction():
        enqueue(conn, "q")
        enqueue(conn, "q")
        assert received(ear, timeout=0.2) == 0  # nothing before the commit
    assert received(ear) == 1  # identical notifications in one transaction are folded


def test_no_notify_for_delayed_jobs_other_queues_or_when_disabled(
    connect: Callable[[], Conn], conn: Conn
) -> None:
    ear = listener(connect, "q")
    enqueue(conn, "q", delay=60 * S)
    enqueue(conn, "other")
    enqueue(conn, "q", notify=False)
    assert received(ear) == 0


def test_reaped_jobs_wake_their_queue(connect: Callable[[], Conn], conn: Conn) -> None:
    enqueue(conn, "q", now=T0)
    claim(conn, "q", "gone", lease=S, now=T0)
    ear = listener(connect, "q")
    assert reap(conn, now=T0 + 2 * S).requeued
    assert received(ear) == 1


def wait_until(predicate: Callable[[], bool], timeout: float) -> float:
    start = time.monotonic()
    while not predicate():
        assert time.monotonic() - start < timeout, "timed out"
        time.sleep(0.005)
    return time.monotonic() - start


def test_idle_worker_wakes_on_notify_long_before_its_poll(
    connect: Callable[[], Conn], conn: Conn
) -> None:
    handled = threading.Event()
    worker = Worker(connect, "q", lambda job, c: handled.set(), worker_id="w", poll_interval=30.0)
    latency: list[float] = []

    def producer() -> None:
        try:
            time.sleep(0.5)  # the worker has found the queue empty and is idling
            enqueue(conn, "q")
            latency.append(wait_until(handled.is_set, timeout=5))
        finally:
            worker.stop()

    started = time.monotonic()
    run_threads(worker.run, producer)
    assert latency[0] < 1.0  # the poll interval is 30 s
    assert worker.stats.wakeups == 1
    assert time.monotonic() - started < 5  # and stop() did not wait out the poll interval


def test_a_lost_notification_only_costs_latency(connect: Callable[[], Conn], conn: Conn) -> None:
    handled = threading.Event()
    worker = Worker(connect, "q", lambda job, c: handled.set(), worker_id="w", poll_interval=0.5)

    def producer() -> None:
        try:
            time.sleep(0.2)
            enqueue(conn, "q", notify=False)  # as if the notification were dropped
            wait_until(handled.is_set, timeout=5)  # found by the next poll
        finally:
            worker.stop()

    run_threads(worker.run, producer)
    assert worker.stats.completed == 1
    assert worker.stats.wakeups == 0


def test_killed_listen_connection_falls_back_to_polling_and_reconnects(
    dsn: str, conn: Conn
) -> None:
    done: list[int] = []

    def handler(job: Job, c: Conn) -> None:
        done.append(job.id)

    connect = lambda: psycopg.connect(dsn, autocommit=True, application_name="listen-test")  # noqa: E731
    worker = Worker(connect, "q", handler, worker_id="w", poll_interval=0.3)

    def saboteur() -> None:
        try:
            time.sleep(0.3)
            rows = conn.execute(
                "SELECT pg_terminate_backend(pid) FROM pg_stat_activity"
                " WHERE application_name = 'listen-test' AND query LIKE 'LISTEN%'"
            ).fetchall()
            assert len(rows) == 1
            first = enqueue(conn, "q")
            wait_until(lambda: first in done, timeout=5)
            time.sleep(0.4)  # long enough to re-LISTEN on the next loop
            second = enqueue(conn, "q")
            wait_until(lambda: second in done, timeout=5)
        finally:
            worker.stop()

    run_threads(worker.run, saboteur)
    assert worker.stats.completed == 2
    assert worker.stats.reconnects >= 1


def test_worker_without_listen_still_polls(connect: Callable[[], Conn], conn: Conn) -> None:
    enqueue(conn, "q")
    worker = Worker(connect, "q", lambda job, c: None, worker_id="w", listen=False)
    assert worker.run(until_empty=True).completed == 1
