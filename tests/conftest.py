"""PostgreSQL for the tests.

PGTASKQ_TEST_DATABASE_URL when set (CI uses a service container), otherwise a throwaway server
from the `pgserver` package (`make install` adds it). Either way the tests work in a schema of
their own, created for the session and dropped afterwards, so pointing the URL at a database
that has real taskq tables cannot touch them.
"""

from __future__ import annotations

import datetime as dt
import os
import secrets
import tempfile
import threading
import time
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any

import psycopg
import pytest
from psycopg import sql
from psycopg.conninfo import make_conninfo

from pg_taskq import migrate

T0 = dt.datetime(2030, 1, 1, tzinfo=dt.UTC)


@pytest.fixture(scope="session")
def server_url() -> Iterator[str]:
    url = os.environ.get("PGTASKQ_TEST_DATABASE_URL")
    if url:
        yield url
        return
    try:
        import pgserver  # type: ignore[import-untyped]
    except ImportError:
        pytest.skip("no PGTASKQ_TEST_DATABASE_URL and the pgserver package is not installed")
    with tempfile.TemporaryDirectory(prefix="pgtaskq-") as tmp:
        server = pgserver.get_server(Path(tmp) / "pgdata", cleanup_mode="stop")
        try:
            yield str(server.get_uri())
        finally:
            server.cleanup()


def in_schema(url: str, schema: str) -> str:
    return make_conninfo(url, options=f"-csearch_path={schema}")


@pytest.fixture(scope="session")
def dsn(server_url: str) -> Iterator[str]:
    """A connection string whose search_path is a fresh, session-private schema."""
    schema = f"taskq_test_{secrets.token_hex(4)}"
    with psycopg.connect(server_url, autocommit=True) as conn:
        conn.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
    try:
        yield in_schema(server_url, schema)
    finally:
        with psycopg.connect(server_url, autocommit=True) as conn:
            conn.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema)))


@pytest.fixture(scope="session")
def _schema(dsn: str) -> None:
    with psycopg.connect(dsn, autocommit=True) as conn:
        migrate(conn)


def _truncate(dsn: str) -> None:
    with psycopg.connect(dsn, autocommit=True) as conn:
        conn.execute("TRUNCATE taskq_jobs, taskq_outbox RESTART IDENTITY")


@pytest.fixture
def connect(dsn: str, _schema: None) -> Iterator[Callable[[], psycopg.Connection[Any]]]:
    """A factory for fresh autocommit connections to a database with empty tables."""
    _truncate(dsn)
    opened: list[psycopg.Connection[Any]] = []

    def factory() -> psycopg.Connection[Any]:
        conn = psycopg.connect(dsn, autocommit=True)
        opened.append(conn)
        return conn

    yield factory
    for conn in opened:
        conn.close()


@pytest.fixture
def conn(connect: Callable[[], psycopg.Connection[Any]]) -> psycopg.Connection[Any]:
    return connect()


def run_threads(*targets: Callable[[], None], timeout: float = 60.0) -> None:
    """Run the targets in parallel threads; re-raise the first exception in the caller
    (an assertion failing inside a plain thread would only produce a warning). A thread still
    running after `timeout` seconds fails the test instead of hanging it."""
    errors: list[BaseException] = []

    def wrap(target: Callable[[], None]) -> Callable[[], None]:
        def run() -> None:
            try:
                target()
            except BaseException as exc:
                errors.append(exc)

        return run

    threads = [threading.Thread(target=wrap(t), daemon=True) for t in targets]
    for t in threads:
        t.start()
    deadline = time.monotonic() + timeout
    for t in threads:
        t.join(max(0.0, deadline - time.monotonic()))
    if errors:
        raise errors[0]
    if any(t.is_alive() for t in threads):
        raise AssertionError(f"threads still running after {timeout} s")
