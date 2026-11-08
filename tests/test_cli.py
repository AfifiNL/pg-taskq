from __future__ import annotations

import json
from collections.abc import Callable
from typing import Any

import psycopg
import pytest

from pg_taskq.cli import main

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


def test_cli_requires_migrated_schema(dsn: str, capsys: pytest.CaptureFixture[str]) -> None:
    with psycopg.connect(dsn, autocommit=True) as conn:
        conn.execute("CREATE SCHEMA IF NOT EXISTS empty_one")
    assert (
        main(
            [
                "--dsn",
                dsn + ("&" if "?" in dsn else "?") + "options=-csearch_path%3Dempty_one",
                "stats",
            ]
        )
        == 2
    )
    assert "run `pg-taskq migrate`" in capsys.readouterr().err
