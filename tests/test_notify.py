"""LISTEN/NOTIFY wake-ups (opt-in: enqueue(notify=True) + Worker(listen=True)), and the polling
fallback when a notification never arrives."""

from __future__ import annotations

import datetime as dt
import secrets
import socket
import threading
import time
from collections.abc import Callable
from typing import Any

import psycopg
import pytest
from psycopg import sql

import pg_taskq.jobs
from pg_taskq import Job, Worker, channel_for, claim, enqueue, migrate, reap
from pg_taskq.jobs import CHANNEL_SQL
from pg_taskq.worker import WorkerStats, _Link, harden_socket

from .conftest import T0, in_schema, run_threads

Conn = psycopg.Connection[Any]
S = dt.timedelta(seconds=1)


def channel_of(c: Conn, queue: str) -> str:
    row = c.execute(f"SELECT {CHANNEL_SQL.format(queue='%s')}", (queue,)).fetchone()
    assert row is not None
    return str(row[0])


def listener(connect: Callable[[], Conn], queue: str) -> Conn:
    c = connect()
    c.execute(sql.SQL("LISTEN {}").format(sql.Identifier(channel_of(c, queue))))
    return c


def received(c: Conn, timeout: float = 0.5) -> int:
    return sum(1 for _ in c.notifies(timeout=timeout))


def wait_until(predicate: Callable[[], bool], timeout: float) -> float:
    start = time.monotonic()
    while not predicate():
        assert time.monotonic() - start < timeout, "timed out"
        time.sleep(0.005)
    return time.monotonic() - start


@pytest.mark.parametrize("queue", ["q", "Queue With Spaces", 'quote"s', "lång-ünïcode", "x" * 300])
def test_channel_names_are_safe_identifiers(conn: Conn, queue: str) -> None:
    channel = channel_for(queue)
    assert channel.startswith("taskq_")
    assert len(channel) <= 63  # PostgreSQL's identifier limit
    assert channel.replace("_", "").isalnum()
    assert channel == channel.lower()
    assert channel != channel_for(queue + "!")
    assert channel != channel_for(queue, schema="other")
    # The Python helper and the SQL that enqueue() and the worker use agree.
    schema = conn.execute("SELECT current_schema()").fetchone()
    assert schema is not None
    assert channel_of(conn, queue) == channel_for(queue, schema=schema[0])


def test_notify_is_opt_in(connect: Callable[[], Conn], conn: Conn) -> None:
    ear = listener(connect, "q")
    enqueue(conn, "q")
    assert received(ear) == 0
    enqueue(conn, "q", notify=True)
    assert received(ear) == 1


def test_enqueue_notifies_on_commit_only(connect: Callable[[], Conn], conn: Conn) -> None:
    ear = listener(connect, "q")
    with pytest.raises(RuntimeError), conn.transaction():  # noqa: PT012 (rollback test)
        enqueue(conn, "q", notify=True)
        raise RuntimeError("rolled back")
    assert received(ear) == 0
    with conn.transaction():
        enqueue(conn, "q", notify=True)
        enqueue(conn, "q", notify=True)
        assert received(ear, timeout=0.2) == 0  # nothing before the commit
    assert received(ear) == 1  # identical notifications in one transaction are folded


def test_no_notify_for_delayed_jobs_or_other_queues(
    connect: Callable[[], Conn], conn: Conn
) -> None:
    ear = listener(connect, "q")
    enqueue(conn, "q", delay=60 * S, notify=True)
    enqueue(conn, "other", notify=True)
    assert received(ear) == 0


def test_reaped_jobs_wake_their_queue(connect: Callable[[], Conn], conn: Conn) -> None:
    enqueue(conn, "q", now=T0)
    claim(conn, "q", "gone", lease=S, now=T0)
    ear = listener(connect, "q")
    assert reap(conn, now=T0 + 2 * S).requeued
    assert received(ear) == 1


@pytest.mark.usefixtures("_schema")
def test_schemas_do_not_wake_each_other(server_url: str, dsn: str) -> None:
    other = f"taskq_other_{secrets.token_hex(4)}"
    with psycopg.connect(server_url, autocommit=True) as c:
        c.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(other)))
    try:
        with (
            psycopg.connect(in_schema(server_url, other), autocommit=True) as b,
            psycopg.connect(dsn, autocommit=True) as a,
            psycopg.connect(dsn, autocommit=True) as ear,
        ):
            migrate(b)
            ear.execute(sql.SQL("LISTEN {}").format(sql.Identifier(channel_of(ear, "q"))))
            assert channel_of(a, "q") != channel_of(b, "q")
            enqueue(b, "q", notify=True)  # same queue name, other schema
            assert received(ear) == 0
            enqueue(a, "q", notify=True)
            assert received(ear) == 1
    finally:
        with psycopg.connect(server_url, autocommit=True) as c:
            c.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(other)))


def test_idle_worker_wakes_on_notify_long_before_its_poll(
    connect: Callable[[], Conn], conn: Conn
) -> None:
    handled = threading.Event()
    worker = Worker(
        connect, "q", lambda job, c: handled.set(), worker_id="w", poll_interval=30.0, listen=True
    )
    latency: list[float] = []

    def producer() -> None:
        try:
            time.sleep(0.5)  # the worker has found the queue empty and is idling
            enqueue(conn, "q", notify=True)
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
    worker = Worker(
        connect, "q", lambda job, c: handled.set(), worker_id="w", poll_interval=0.5, listen=True
    )

    def producer() -> None:
        try:
            time.sleep(0.2)
            enqueue(conn, "q")  # no notification, as if it had been dropped
            wait_until(handled.is_set, timeout=5)  # found by the next poll
        finally:
            worker.stop()

    run_threads(worker.run, producer)
    assert worker.stats.completed == 1
    assert worker.stats.wakeups == 0


def test_listen_is_reissued_after_the_connection_is_killed(dsn: str, conn: Conn) -> None:
    done: list[int] = []
    connect = lambda: psycopg.connect(dsn, autocommit=True, application_name="listen-test")  # noqa: E731
    worker = Worker(
        connect,
        "q",
        lambda job, c: done.append(job.id),
        worker_id="w",
        poll_interval=30.0,  # only a working LISTEN can make the pickups below prompt
        listen=True,
    )

    def saboteur() -> None:
        try:
            first = enqueue(conn, "q", notify=True)
            wait_until(lambda: first in done, timeout=5)
            rows = conn.execute(
                "SELECT pg_terminate_backend(pid) FROM pg_stat_activity"
                " WHERE application_name = 'listen-test' AND query LIKE 'LISTEN%'"
            ).fetchall()
            assert len(rows) == 1
            wait_until(lambda: worker.stats.reconnects >= 1, timeout=5)
            time.sleep(0.2)
            before.append(worker.stats.wakeups)
            second = enqueue(conn, "q", notify=True)
            wait_until(lambda: second in done, timeout=3)
        finally:
            worker.stop()

    before: list[int] = []
    run_threads(worker.run, saboteur)
    assert worker.stats.completed == 2
    assert worker.stats.wakeups == before[0] + 1  # the second pickup came from the new LISTEN


def test_listen_happens_before_the_claim(
    connect: Callable[[], Conn], conn: Conn, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A job committed between an empty claim and the wait must still wake the worker."""
    real_claim = pg_taskq.jobs.claim
    injected = threading.Event()
    other = connect()

    def claim_then_enqueue(*args: Any, **kwargs: Any) -> list[Job]:
        got = real_claim(*args, **kwargs)
        if not got and not injected.is_set():
            injected.set()
            enqueue(other, "q", notify=True)  # lands in the window after the empty claim
        return got

    monkeypatch.setattr(pg_taskq.jobs, "claim", claim_then_enqueue)
    handled = threading.Event()
    worker = Worker(
        connect, "q", lambda job, c: handled.set(), worker_id="w", poll_interval=30.0, listen=True
    )

    def watcher() -> None:
        try:
            wait_until(handled.is_set, timeout=3)
        finally:
            worker.stop()

    run_threads(worker.run, watcher)
    assert injected.is_set()
    assert worker.stats.wakeups == 1


def test_the_listen_connection_is_checked_periodically(
    connect: Callable[[], Conn], conn: Conn
) -> None:
    worker = Worker(
        connect,
        "q",
        lambda job, c: None,
        worker_id="w",
        poll_interval=0.2,
        listen=True,
        listen_check_every=0.1,
    )

    def stopper() -> None:
        time.sleep(0.8)
        worker.stop()

    run_threads(worker.run, stopper)
    assert worker.stats.listen_checks >= 2


def test_worker_without_listen_still_polls(connect: Callable[[], Conn], conn: Conn) -> None:
    enqueue(conn, "q", notify=True)
    worker = Worker(connect, "q", lambda job, c: None, worker_id="w")
    assert worker.run(until_empty=True).completed == 1


def test_keepalives_are_set_on_tcp_sockets(conn: Conn) -> None:
    server = socket.create_server(("127.0.0.1", 0))
    client = socket.create_connection(server.getsockname())
    try:

        class Fake:
            class pgconn:  # mimics psycopg's attribute
                socket = client.fileno()

        assert harden_socket(Fake())
        assert client.getsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE) == 1
        if hasattr(socket, "TCP_KEEPIDLE"):
            assert client.getsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPIDLE) == 30
    finally:
        client.close()
        server.close()
    unix_or_tcp = harden_socket(conn)  # the test database may use either
    assert isinstance(unix_or_tcp, bool)


def test_a_failed_listen_does_not_leak_the_connection(connect: Callable[[], Conn]) -> None:
    opened: list[Conn] = []

    def factory() -> Conn:
        opened.append(connect())
        return opened[-1]

    def broken_listen(c: Conn) -> None:
        raise psycopg.OperationalError("LISTEN refused")

    link = _Link(factory, WorkerStats(), on_connect=broken_listen)
    with pytest.raises(psycopg.OperationalError):
        link.get()
    assert len(opened) == 1
    assert opened[0].closed
    assert not link.connected
