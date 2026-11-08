from __future__ import annotations

import datetime as dt
import threading
import time
from collections.abc import Callable
from typing import Any

import psycopg
import pytest

from pg_taskq import Backoff, outbox
from pg_taskq.outbox import OutboxError, OutboxMessage

from .conftest import run_threads

Conn = psycopg.Connection[Any]
S = dt.timedelta(seconds=1)


def test_message_exists_only_if_the_transaction_commits(conn: Conn) -> None:
    with pytest.raises(RuntimeError), conn.transaction():  # noqa: PT012 (rollback test)
        outbox.add(conn, "order.created", {"order": 1})
        raise RuntimeError("business write failed")
    assert outbox.pending(conn) == 0
    with conn.transaction():
        outbox.add(conn, "order.created", {"order": 2}, key="customer-9")
    assert outbox.pending(conn) == 1


def test_add_refuses_autocommit_outside_a_transaction(conn: Conn) -> None:
    with pytest.raises(OutboxError, match="inside the transaction"):
        outbox.add(conn, "t", {})


@pytest.mark.usefixtures("_schema")
def test_add_works_in_an_implicit_transaction(dsn: str) -> None:
    with psycopg.connect(dsn) as conn:  # autocommit off: statements open a transaction
        conn.execute("TRUNCATE taskq_outbox")
        conn.commit()
        outbox.add(conn, "t", {"x": 1})
        conn.rollback()
        assert outbox.pending(conn) == 0
        outbox.add(conn, "t", {"x": 2})
        conn.commit()
        assert outbox.pending(conn) == 1


def test_relay_publishes_in_order_and_marks_published(conn: Conn) -> None:
    with conn.transaction():
        ids = [outbox.add(conn, "t", {"n": n}) for n in range(5)]
    seen: list[OutboxMessage] = []
    result = outbox.relay_once(conn, seen.append)
    assert [m.id for m in seen] == ids == result.published
    assert [m.payload for m in seen] == [{"n": n} for n in range(5)]
    assert outbox.pending(conn) == 0
    assert outbox.relay_once(conn, seen.append).published == []


def test_failed_publish_is_retried_later_and_does_not_block_the_batch(conn: Conn) -> None:
    with conn.transaction():
        bad = outbox.add(conn, "t", {"bad": True})
        good = outbox.add(conn, "t", {"bad": False})

    def publish(msg: OutboxMessage) -> None:
        if msg.payload["bad"]:
            raise ConnectionError("broker down")

    backoff = Backoff(base=10 * S, cap=60 * S)
    now = dt.datetime.now(dt.UTC)
    result = outbox.relay_once(conn, publish, backoff=backoff, now=now)
    assert result.published == [good]
    assert result.failed == [bad]
    row = conn.execute(
        "SELECT attempts, last_error, available_at FROM taskq_outbox WHERE id = %s", (bad,)
    ).fetchone()
    assert row is not None
    assert row[0] == 1
    assert row[1] == "ConnectionError: broker down"
    assert now + 5 * S <= row[2] <= now + 10 * S
    assert outbox.relay_once(conn, publish, now=now + 4 * S).failed == []  # not due yet
    retried = outbox.relay_once(conn, lambda m: None, now=now + 10 * S)
    assert retried.published == [bad]


def test_parallel_relays_take_disjoint_batches(connect: Callable[[], Conn]) -> None:
    setup = connect()
    with setup.transaction():
        all_ids = [outbox.add(setup, "t", {"n": n}) for n in range(60)]
    published: list[int] = []
    lock = threading.Lock()

    def publish(msg: OutboxMessage) -> None:
        time.sleep(0.002)  # hold the batch long enough for the relays to overlap
        with lock:
            published.append(msg.id)

    def relay() -> None:
        with connect() as conn:
            while outbox.relay_once(conn, publish, limit=7).published:
                pass

    run_threads(*[relay for _ in range(4)])
    assert sorted(published) == all_ids  # each message exactly once in a crash-free run


def test_relay_crash_mid_batch_republishes(conn: Conn) -> None:
    with conn.transaction():
        ids = [outbox.add(conn, "t", {}) for _ in range(3)]
    calls: list[int] = []

    class Crash(BaseException):  # not an Exception: simulates the process dying
        pass

    def crashing(msg: OutboxMessage) -> None:
        calls.append(msg.id)
        if len(calls) == 2:
            raise Crash

    with pytest.raises(Crash):
        outbox.relay_once(conn, crashing)
    assert outbox.pending(conn) == 3  # nothing was marked: the first message goes out again
    result = outbox.relay_once(conn, lambda m: calls.append(m.id))
    assert result.published == ids
    assert calls.count(ids[0]) == 2  # at-least-once: consumers deduplicate by id


@pytest.mark.parametrize("text", ["broker said: \x00", "bad \udc80 byte"])
def test_unstorable_publisher_errors_do_not_wedge_the_relay(conn: Conn, text: str) -> None:
    with conn.transaction():
        first = outbox.add(conn, "t", {"n": 1})
        bad = outbox.add(conn, "t", {"n": 2})
    sent: list[int] = []

    def publish(msg: OutboxMessage) -> None:
        if msg.id == bad:
            raise RuntimeError(text)
        sent.append(msg.id)

    result = outbox.relay_once(conn, publish)
    assert result.published == [first]
    assert result.failed == [bad]
    row = conn.execute("SELECT last_error FROM taskq_outbox WHERE id = %s", (bad,)).fetchone()
    assert row is not None
    assert "\\x00" in row[0] or "\\udc80" in row[0]
    # The first message was marked published: a second relay does not resend it.
    assert outbox.relay_once(conn, publish).published == []
    assert sent == [first]
