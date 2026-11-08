"""Migrating a version-1 schema while application transactions hold locks on its tables."""

from __future__ import annotations

import datetime as dt
import secrets
import threading
import time
from collections.abc import Callable, Iterator
from typing import Any

import psycopg
import pytest
from psycopg import errors, sql

from pg_taskq import (
    MigrationError,
    MigrationLockError,
    enqueue,
    migrate,
    outbox,
    schema_version,
)
from pg_taskq.schema import LATEST, MIGRATIONS

from .conftest import in_schema, run_threads

Conn = psycopg.Connection[Any]
FAST = {"lock_timeout": dt.timedelta(milliseconds=200), "attempts": 50}


@pytest.fixture
def v1(server_url: str) -> Iterator[Callable[[], Conn]]:
    """A fresh schema at version 1, and a factory for autocommit connections to it."""
    schema = f"taskq_mig_{secrets.token_hex(4)}"
    with psycopg.connect(server_url, autocommit=True) as c:
        c.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
    url = in_schema(server_url, schema)
    opened: list[Conn] = []

    def connect() -> Conn:
        opened.append(psycopg.connect(url, autocommit=True))
        return opened[-1]

    c = connect()
    c.execute(
        "CREATE TABLE taskq_schema_version (version integer PRIMARY KEY,"
        " applied_at timestamptz NOT NULL DEFAULT statement_timestamp())"
    )
    c.execute(MIGRATIONS[0][1].encode())
    c.execute("INSERT INTO taskq_schema_version (version) VALUES (1)")
    try:
        yield connect
    finally:
        for conn in opened:
            conn.close()
        with psycopg.connect(server_url, autocommit=True) as c:
            c.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema)))


def indexes(c: Conn) -> set[str]:
    rows = c.execute(
        "SELECT indexname FROM pg_indexes WHERE schemaname = current_schema()"
    ).fetchall()
    return {str(r[0]) for r in rows}


def test_migration_from_version_1(v1: Callable[[], Conn]) -> None:
    c = v1()
    assert schema_version(c) == 1
    assert migrate(c) == [2]
    assert schema_version(c) == LATEST == 2
    assert {"taskq_jobs_finished", "taskq_outbox_published"} <= indexes(c)
    assert migrate(c) == []


@pytest.mark.parametrize("outbox_first", [True, False])
def test_migration_does_not_deadlock_with_application_transactions(
    v1: Callable[[], Conn], outbox_first: bool
) -> None:
    """The README's handler pattern (outbox.add, then a write to taskq_jobs) and the opposite
    order, each in a transaction that is open while migrate() runs. Before the fix, the first
    order deadlocked (DeadlockDetected) against the migration's index builds."""
    app, migrator = v1(), v1()
    result: list[list[int]] = []
    holding = threading.Event()
    release = threading.Event()

    def application() -> None:
        with app.transaction():
            if outbox_first:
                outbox.add(app, "t", {"n": 1})
            else:
                enqueue(app, "q")
            holding.set()
            assert release.wait(5)
            # Now touch the other table while migrate() is (or was) trying to lock both.
            if outbox_first:
                enqueue(app, "q")
            else:
                outbox.add(app, "t", {"n": 1})

    def migration() -> None:
        assert holding.wait(5)
        result.append(migrate(migrator, **FAST))  # type: ignore[arg-type]

    def timer() -> None:
        assert holding.wait(5)
        time.sleep(0.5)  # let migrate() try (and back off) a few times
        release.set()

    run_threads(application, migration, timer)  # a deadlock would raise in one of them
    assert result == [[2]]
    assert schema_version(app) == 2


def test_migration_gives_up_with_a_clear_error(v1: Callable[[], Conn]) -> None:
    app, migrator = v1(), v1()
    with app.transaction():
        outbox.add(app, "t", {})  # holds a lock the migration needs, for the whole attempt
        with pytest.raises(MigrationLockError, match="stayed locked"):
            migrate(migrator, lock_timeout=dt.timedelta(milliseconds=50), attempts=3)
    assert schema_version(migrator) == 1
    assert migrate(migrator) == [2]


def test_indexes_built_concurrently_beforehand(v1: Callable[[], Conn]) -> None:
    c = v1()
    c.execute(
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS taskq_jobs_finished"
        " ON taskq_jobs (finished_at) WHERE state IN ('done', 'dead')"
    )
    c.execute(
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS taskq_outbox_published"
        " ON taskq_outbox (published_at) WHERE published_at IS NOT NULL"
    )
    assert migrate(c) == [2]


def test_an_invalid_prebuilt_index_is_refused(v1: Callable[[], Conn]) -> None:
    c = v1()
    enqueue(c, "same")
    enqueue(c, "same")
    with pytest.raises(errors.UniqueViolation):  # fails and leaves an INVALID index behind
        c.execute("CREATE UNIQUE INDEX CONCURRENTLY taskq_jobs_finished ON taskq_jobs (queue)")
    with pytest.raises(MigrationError, match="INVALID"):
        migrate(c)
    assert schema_version(c) == 1
    c.execute("DROP INDEX taskq_jobs_finished")
    assert migrate(c) == [2]


def test_migrate_validates_arguments(v1: Callable[[], Conn]) -> None:
    with pytest.raises(ValueError, match="attempts"):
        migrate(v1(), attempts=0)


def test_migration_never_waits_while_holding_a_lock(v1: Callable[[], Conn]) -> None:
    """With the first table free and the second locked, migrate() must give up on the second
    at once (NOWAIT) rather than wait out lock_timeout while holding the first."""
    app, migrator = v1(), v1()
    with app.transaction():
        outbox.add(app, "t", {})
        started = time.monotonic()
        with pytest.raises(MigrationLockError):
            migrate(migrator, lock_timeout=dt.timedelta(seconds=5), attempts=1)
        assert time.monotonic() - started < 2
