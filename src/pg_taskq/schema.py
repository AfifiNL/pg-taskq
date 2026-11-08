"""Schema and migrations.

Tables live in the connection's current schema (set `search_path` to place them elsewhere).
Migrations are numbered, applied in order inside one transaction, and serialised with a
transaction-scoped advisory lock, so two processes starting at once cannot both apply one.

Locks. A migration that changes existing tables first takes SHARE locks on them, which blocks
writes (enqueue, claim, heartbeat, complete) until the migration commits. To keep that safe:

* it waits only for the *first* table's lock, while holding no lock a job transaction could
  need, and takes every further lock with NOWAIT, so it can never be part of a deadlock with
  application transactions that touch the tables in either order;
* every lock wait is bounded by `lock_timeout`; a timeout or a NOWAIT miss rolls back and
  retries with backoff, and after `attempts` tries `MigrationLockError` says why.

Call migrate() on an idle connection, at deploy time rather than in every worker if your tables
are large: index builds hold those locks for as long as they run. Indexes can be built beforehand
with CREATE INDEX CONCURRENTLY (see the README); migrations use IF NOT EXISTS and then only
check that the index is valid.
"""

from __future__ import annotations

import datetime as dt
import random
import time
from typing import Any

import psycopg
from psycopg import errors, sql

# Arbitrary constant for pg_advisory_xact_lock: "taskq" as an integer.
_MIGRATION_LOCK = 0x7461736B71


class MigrationError(RuntimeError):
    pass


class MigrationLockError(MigrationError):
    """The tables stayed locked by other transactions for every attempt."""


# (version, DDL, tables to lock first, indexes that must be valid afterwards)
MIGRATIONS: list[tuple[int, str, tuple[str, ...], tuple[str, ...]]] = [
    (
        1,
        """
        CREATE TABLE taskq_jobs (
            id              bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            queue           text        NOT NULL,
            payload         jsonb       NOT NULL DEFAULT '{}'::jsonb,
            priority        integer     NOT NULL DEFAULT 0,
            state           text        NOT NULL DEFAULT 'queued'
                            CHECK (state IN ('queued', 'running', 'done', 'dead')),
            attempts        integer     NOT NULL DEFAULT 0,
            max_attempts    integer     NOT NULL DEFAULT 5 CHECK (max_attempts >= 1),
            run_at          timestamptz NOT NULL,
            idempotency_key text,
            lease_token     uuid,
            locked_by       text,
            locked_until    timestamptz,
            last_error      text,
            created_at      timestamptz NOT NULL,
            updated_at      timestamptz NOT NULL,
            finished_at     timestamptz,
            CONSTRAINT taskq_attempts_bounded CHECK (attempts >= 0 AND attempts <= max_attempts),
            CONSTRAINT taskq_lease_iff_running CHECK (
                (state = 'running') = (lease_token IS NOT NULL
                                       AND locked_by IS NOT NULL
                                       AND locked_until IS NOT NULL)
            ),
            CONSTRAINT taskq_finished_iff_final CHECK (
                (state IN ('done', 'dead')) = (finished_at IS NOT NULL)
            )
        );
        CREATE UNIQUE INDEX taskq_jobs_idempotency
            ON taskq_jobs (queue, idempotency_key) WHERE idempotency_key IS NOT NULL;
        CREATE INDEX taskq_jobs_ready
            ON taskq_jobs (queue, priority DESC, run_at, id) WHERE state = 'queued';
        CREATE INDEX taskq_jobs_leases
            ON taskq_jobs (locked_until) WHERE state = 'running';

        CREATE TABLE taskq_outbox (
            id            bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            topic         text        NOT NULL,
            message_key   text,
            payload       jsonb       NOT NULL,
            created_at    timestamptz NOT NULL DEFAULT statement_timestamp(),
            available_at  timestamptz NOT NULL DEFAULT statement_timestamp(),
            published_at  timestamptz,
            attempts      integer     NOT NULL DEFAULT 0,
            last_error    text
        );
        CREATE INDEX taskq_outbox_pending
            ON taskq_outbox (available_at, id) WHERE published_at IS NULL;
        """,
        (),
        (),
    ),
    (
        2,
        """
        -- For prune(): finished jobs and published messages by age. IF NOT EXISTS so that
        -- they can be built beforehand with CREATE INDEX CONCURRENTLY.
        CREATE INDEX IF NOT EXISTS taskq_jobs_finished
            ON taskq_jobs (finished_at) WHERE state IN ('done', 'dead');
        CREATE INDEX IF NOT EXISTS taskq_outbox_published
            ON taskq_outbox (published_at) WHERE published_at IS NOT NULL;
        """,
        ("taskq_jobs", "taskq_outbox"),
        ("taskq_jobs_finished", "taskq_outbox_published"),
    ),
]

LATEST = MIGRATIONS[-1][0]


def migrate(
    conn: psycopg.Connection[Any],
    *,
    lock_timeout: dt.timedelta = dt.timedelta(seconds=1),
    attempts: int = 30,
) -> list[int]:
    """Apply every migration not yet applied. Returns the versions applied (empty if none)."""
    if attempts < 1 or lock_timeout <= dt.timedelta(0):
        raise ValueError("attempts must be >= 1 and lock_timeout positive")
    for attempt in range(1, attempts + 1):
        try:
            return _migrate_once(conn, lock_timeout)
        except (errors.LockNotAvailable, errors.DeadlockDetected) as exc:
            if attempt == attempts:
                raise MigrationLockError(
                    f"tables stayed locked by other transactions for {attempts} attempts "
                    f"({type(exc).__name__}); retry when the load is lower, or build the "
                    "migration's indexes CONCURRENTLY first"
                ) from exc
            # Not cryptographic: jitter only spreads out competing migrators.
            time.sleep(min(2.0, 0.05 * 2**attempt) * (0.5 + random.random() / 2))  # noqa: S311
    raise AssertionError("unreachable")  # pragma: no cover


def _migrate_once(conn: psycopg.Connection[Any], lock_timeout: dt.timedelta) -> list[int]:
    applied: list[int] = []
    with conn.transaction():
        # Wait for another migrator without a timeout: this transaction holds nothing yet.
        conn.execute("SELECT pg_advisory_xact_lock(%s)", (_MIGRATION_LOCK,))
        timeout_ms = max(1, int(lock_timeout.total_seconds() * 1000))
        conn.execute("SELECT set_config('lock_timeout', %s, true)", (f"{timeout_ms}ms",))
        conn.execute(
            "CREATE TABLE IF NOT EXISTS taskq_schema_version ("
            " version integer PRIMARY KEY,"
            " applied_at timestamptz NOT NULL DEFAULT statement_timestamp())"
        )
        row = conn.execute("SELECT coalesce(max(version), 0) FROM taskq_schema_version").fetchone()
        current = int(row[0]) if row else 0
        for version, ddl, tables, indexes in MIGRATIONS:
            if version <= current:
                continue
            for i, table in enumerate(tables):
                # Wait (bounded) only for the first lock; never wait while holding one.
                mode = "IN SHARE MODE" if i == 0 else "IN SHARE MODE NOWAIT"
                conn.execute(sql.SQL("LOCK TABLE {} " + mode).format(sql.Identifier(table)))
            conn.execute(ddl.encode())
            _check_valid(conn, indexes)
            conn.execute("INSERT INTO taskq_schema_version (version) VALUES (%s)", (version,))
            applied.append(version)
    return applied


def _check_valid(conn: psycopg.Connection[Any], indexes: tuple[str, ...]) -> None:
    """An index left INVALID by a failed CREATE INDEX CONCURRENTLY would satisfy IF NOT EXISTS
    but is never used; refuse to record the migration on top of it."""
    for name in indexes:
        row = conn.execute(
            "SELECT i.indisvalid FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid"
            " WHERE c.oid = to_regclass(%s)",
            (name,),
        ).fetchone()
        if row is None or not row[0]:
            raise MigrationError(
                f"index {name} is missing or INVALID (a failed CREATE INDEX CONCURRENTLY?): "
                f"DROP INDEX {name}; then run migrate again"
            )


def schema_version(conn: psycopg.Connection[Any]) -> int:
    """The highest applied migration, or 0 when the schema is not installed."""
    row = conn.execute("SELECT to_regclass('taskq_schema_version') IS NOT NULL").fetchone()
    if not row or not row[0]:
        return 0
    row = conn.execute("SELECT coalesce(max(version), 0) FROM taskq_schema_version").fetchone()
    return int(row[0]) if row else 0
