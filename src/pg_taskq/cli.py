"""Command line: `pg-taskq migrate | enqueue | stats | reap`.

The connection string comes from `--dsn` or the PGTASKQ_DSN environment variable.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import sys
from collections.abc import Sequence

import psycopg

from pg_taskq import jobs, outbox
from pg_taskq.schema import LATEST, migrate, schema_version


def _parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="pg-taskq", description=__doc__)
    p.add_argument("--dsn", default=os.environ.get("PGTASKQ_DSN"), help="PostgreSQL URI")
    sub = p.add_subparsers(dest="cmd", required=True)
    sub.add_parser("migrate", help="create or upgrade the tables")
    e = sub.add_parser("enqueue", help="add one job")
    e.add_argument("queue")
    e.add_argument("payload", nargs="?", default="{}", help="JSON payload (default {})")
    e.add_argument("--delay", type=float, default=0.0, help="seconds before it is due")
    e.add_argument("--priority", type=int, default=0)
    e.add_argument("--max-attempts", type=int, default=5)
    e.add_argument("--key", default=None, help="idempotency key (unique per queue)")
    sub.add_parser("stats", help="job counts per queue and state, pending outbox messages")
    sub.add_parser("reap", help="requeue jobs whose lease expired")
    return p


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    if not args.dsn:
        print("error: pass --dsn or set PGTASKQ_DSN", file=sys.stderr)
        return 2
    with psycopg.connect(args.dsn, autocommit=True) as conn:
        if args.cmd == "migrate":
            applied = migrate(conn)
            print(f"schema at version {LATEST}" + (f" (applied {applied})" if applied else ""))
            return 0
        if schema_version(conn) < LATEST:
            print("error: schema missing or outdated; run `pg-taskq migrate`", file=sys.stderr)
            return 2
        if args.cmd == "enqueue":
            try:
                payload = json.loads(args.payload)
            except json.JSONDecodeError as exc:
                print(f"error: payload is not JSON: {exc}", file=sys.stderr)
                return 2
            job_id = jobs.enqueue(
                conn,
                args.queue,
                payload,
                delay=dt.timedelta(seconds=args.delay),
                priority=args.priority,
                max_attempts=args.max_attempts,
                idempotency_key=args.key,
            )
            print(job_id)
            return 0
        if args.cmd == "stats":
            print(json.dumps({"jobs": jobs.stats(conn), "outbox_pending": outbox.pending(conn)}))
            return 0
        if args.cmd == "reap":
            reaped = jobs.reap(conn)
            print(json.dumps({"requeued": reaped.requeued, "dead": reaped.dead}))
            return 0
    return 2  # pragma: no cover - argparse rejects unknown commands


if __name__ == "__main__":
    sys.exit(main())
