from __future__ import annotations

import datetime as dt
import json
import secrets
import time
from collections.abc import Callable
from typing import Any

import psycopg
import pytest
from psycopg import sql

from pg_taskq import claim, enqueue
from pg_taskq.cli import main

from .conftest import in_schema

Conn = psycopg.Connection[Any]


def test_cli_roundtrip(dsn: str, conn: Conn, capsys: pytest.CaptureFixture[str]) -> None:
    assert main(["--dsn", dsn, "migrate"]) == 0
    assert "schema at version" in capsys.readouterr().out
    assert main(["--dsn", dsn, "enqueue", "reports", '{"month": "2030-01"}', "--key", "m1"]) == 0
    first = capsys.readouterr().out.strip()
    assert main(["--dsn", dsn, "enqueue", "reports", "{}", "--key", "m1"]) == 0
    assert capsys.readouterr().out.strip() == first
    assert main(["--dsn", dsn, "stats"]) == 0
    assert json.loads(capsys.readouterr().out) == {
        "jobs": {"reports": {"queued": 1}},
        "outbox_pending": 0,
    }
    assert main(["--dsn", dsn, "reap"]) == 0
    assert json.loads(capsys.readouterr().out) == {"requeued": [], "dead": []}


def test_cli_errors(
    dsn: str,
    connect: Callable[[], Conn],
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("PGTASKQ_DSN", raising=False)
    assert main(["stats"]) == 2
    assert "PGTASKQ_DSN" in capsys.readouterr().err
    assert main(["--dsn", dsn, "enqueue", "q", "{not json"]) == 2
    assert "not JSON" in capsys.readouterr().err


def test_cli_requires_migrated_schema(server_url: str, capsys: pytest.CaptureFixture[str]) -> None:
    schema = f"taskq_empty_{secrets.token_hex(4)}"
    with psycopg.connect(server_url, autocommit=True) as c:
        c.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
    try:
        assert main(["--dsn", in_schema(server_url, schema), "stats"]) == 2
        assert "run `pg-taskq migrate`" in capsys.readouterr().err
    finally:
        with psycopg.connect(server_url, autocommit=True) as c:
            c.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema)))


@pytest.mark.parametrize(
    ("args", "message"),
    [
        (["enqueue", "q", "{}", "--max-attempts", "0"], "max_attempts"),
        (["enqueue", "q", "{}", "--delay", "nan"], "--delay"),
        (["enqueue", "q", "{}", "--delay", "-1"], "--delay"),
        (["enqueue", "q", "{}", "--priority", str(2**40)], "priority"),
        (["reap", "--limit", "0"], "--limit"),
    ],
)
def test_cli_bad_arguments_are_clean_errors(
    dsn: str, conn: Conn, capsys: pytest.CaptureFixture[str], args: list[str], message: str
) -> None:
    assert main(["--dsn", dsn, *args]) == 2
    err = capsys.readouterr().err
    assert message in err
    assert "Traceback" not in err


def test_cli_reap_limit(dsn: str, conn: Conn, capsys: pytest.CaptureFixture[str]) -> None:
    for _ in range(3):
        enqueue(conn, "q")
    claim(conn, "q", "gone", limit=3, lease=dt.timedelta(milliseconds=1))
    time.sleep(0.01)
    assert main(["--dsn", dsn, "reap", "--limit", "2"]) == 0
    assert len(json.loads(capsys.readouterr().out)["requeued"]) == 2
