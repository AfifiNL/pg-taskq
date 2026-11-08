"""Transactional outbox: record a message in the same transaction as the change it describes,
then publish it from a relay.

Without an outbox, "write to the database, then publish to the broker" has two failure windows:
the write commits and the publish is lost, or the publish happens and the write rolls back. With
`add()` inside the caller's transaction, the message exists if and only if the change committed.
The relay then publishes committed messages *at least once*: a crash after publishing but before
marking the row published sends it again, so consumers must deduplicate by message id.
"""

from __future__ import annotations

import datetime as dt
from collections.abc import Callable
from dataclasses import dataclass, fields
from typing import Any

import psycopg
from psycopg.pq import TransactionStatus
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from pg_taskq.jobs import NOW, Backoff, check_aware, check_name, describe_error


class OutboxError(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class OutboxMessage:
    id: int
    topic: str
    message_key: str | None
    payload: Any
    created_at: dt.datetime
    attempts: int


@dataclass(frozen=True, slots=True)
class RelayResult:
    published: list[int]
    failed: list[int]


def add(
    conn: psycopg.Connection[Any],
    topic: str,
    payload: Any,
    *,
    key: str | None = None,
) -> int:
    """Record a message in the caller's current transaction and return its id.

    Refuses to run on an autocommit connection outside a transaction block: there the message
    would commit on its own, which defeats the pattern.
    """
    check_name(topic, "topic")
    if key is not None:
        check_name(key, "key")
    if conn.autocommit and conn.info.transaction_status == TransactionStatus.IDLE:
        raise OutboxError(
            "outbox.add() must run inside the transaction that makes the change "
            "(use `with conn.transaction():`)"
        )
    row = conn.execute(
        "INSERT INTO taskq_outbox (topic, message_key, payload) VALUES (%s, %s, %s) RETURNING id",
        (topic, key, Jsonb(payload)),
    ).fetchone()
    if row is None:  # pragma: no cover
        raise OutboxError("insert returned no id")
    return int(row[0])


def relay_once(
    conn: psycopg.Connection[Any],
    publish: Callable[[OutboxMessage], None],
    *,
    limit: int = 100,
    backoff: Backoff | None = None,
    now: dt.datetime | None = None,
) -> RelayResult:
    """Publish up to `limit` due messages, in one transaction.

    Within a batch, messages go out in id order. That is not a delivery-order guarantee: ids are
    assigned at insert, not at commit, so a message from a transaction that commits late can
    follow one with a higher id; a failed message waits for its retry while later ones go out;
    and parallel relays interleave. Consumers that need order must sort or version by payload.

    The rows stay locked (`FOR UPDATE SKIP LOCKED`) while `publish` runs, so parallel relays
    take disjoint batches. A message whose publish raises is not marked published; it gets
    `attempts + 1`, the error, and a backoff delay before the next try. Its failure does not
    stop the rest of the batch. If the relay process dies mid-batch, the transaction rolls back
    and the whole batch is retried (at-least-once).
    """
    check_aware(now, "now")
    if limit < 1:
        raise ValueError("limit must be >= 1")
    policy = backoff or Backoff()
    published: list[int] = []
    failed: list[int] = []
    with conn.transaction(), conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            f"""
            SELECT id, topic, message_key, payload, created_at, attempts FROM taskq_outbox
            WHERE published_at IS NULL AND available_at <= {NOW}
            ORDER BY id
            LIMIT %(limit)s
            FOR UPDATE SKIP LOCKED
            """,  # noqa: S608
            {"limit": limit, "now": now},
        )
        batch = [
            OutboxMessage(**{f.name: r[f.name] for f in fields(OutboxMessage)})
            for r in cur.fetchall()
        ]
        for msg in batch:
            try:
                publish(msg)
            except Exception as exc:  # the publisher's failure is data, not a crash
                cur.execute(
                    f"""
                    UPDATE taskq_outbox
                    SET attempts = attempts + 1, last_error = %(error)s,
                        available_at = {NOW} + %(delay)s
                    WHERE id = %(id)s
                    """,  # noqa: S608
                    {
                        "id": msg.id,
                        "error": describe_error(exc),
                        "delay": policy.delay(msg.attempts + 1),
                        "now": now,
                    },
                )
                failed.append(msg.id)
                continue
            cur.execute(
                f"UPDATE taskq_outbox SET published_at = {NOW}, last_error = NULL "  # noqa: S608
                "WHERE id = %(id)s",
                {"id": msg.id, "now": now},
            )
            published.append(msg.id)
    return RelayResult(published=published, failed=failed)


def pending(conn: psycopg.Connection[Any]) -> int:
    """Messages not yet published (including ones waiting for a retry)."""
    row = conn.execute("SELECT count(*) FROM taskq_outbox WHERE published_at IS NULL").fetchone()
    return int(row[0]) if row else 0
