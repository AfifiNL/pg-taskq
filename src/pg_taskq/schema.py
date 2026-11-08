"""Schema and migrations.

Tables live in the connection's current schema (set `search_path` to place them elsewhere).
Migrations are numbered, applied in order inside one transaction, and serialised with a
transaction-scoped advisory lock, so two processes starting at once cannot both apply one.
"""

from __future__ import annotations

from typing import Any

import psycopg

# Arbitrary constant for pg_advisory_xact_lock: "taskq" as an integer.
_MIGRATION_LOCK = 0x7461736B71

MIGRATIONS: list[tuple[int, str]] = [
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
    ),
]

LATEST = MIGRATIONS[-1][0]


def migrate(conn: psycopg.Connection[Any]) -> list[int]:
    """Apply every migration not yet applied. Returns the versions applied (empty if none)."""
    applied: list[int] = []
    with conn.transaction():
        conn.execute("SELECT pg_advisory_xact_lock(%s)", (_MIGRATION_LOCK,))
        conn.execute(
            "CREATE TABLE IF NOT EXISTS taskq_schema_version ("
            " version integer PRIMARY KEY,"
            " applied_at timestamptz NOT NULL DEFAULT statement_timestamp())"
        )
        row = conn.execute("SELECT coalesce(max(version), 0) FROM taskq_schema_version").fetchone()
        current = int(row[0]) if row else 0
        for version, ddl in MIGRATIONS:
            if version <= current:
                continue
            conn.execute(ddl.encode())
            conn.execute("INSERT INTO taskq_schema_version (version) VALUES (%s)", (version,))
            applied.append(version)
    return applied


def schema_version(conn: psycopg.Connection[Any]) -> int:
    """The highest applied migration, or 0 when the schema is not installed."""
    row = conn.execute("SELECT to_regclass('taskq_schema_version') IS NOT NULL").fetchone()
    if not row or not row[0]:
        return 0
    row = conn.execute("SELECT coalesce(max(version), 0) FROM taskq_schema_version").fetchone()
    return int(row[0]) if row else 0
