"""PostgreSQL for the tests: PGTASKQ_TEST_DATABASE_URL when set (CI uses a service container),
otherwise a throwaway server from the `pgserver` package (`make install` adds it locally)."""

from __future__ import annotations

import datetime as dt
import os
import tempfile
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any

import psycopg
import pytest

from pg_taskq import migrate

T0 = dt.datetime(2030, 1, 1, tzinfo=dt.UTC)


@pytest.fixture(scope="session")
def dsn() -> Iterator[str]:
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


@pytest.fixture(scope="session")
def _schema(dsn: str) -> None:
    with psycopg.connect(dsn, autocommit=True) as conn:
        conn.execute("DROP TABLE IF EXISTS taskq_jobs, taskq_outbox, taskq_schema_version")
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


@pytest.fixture
def truncate(dsn: str) -> Callable[[], None]:
    return lambda: _truncate(dsn)
