"""Command line: `pg-taskq migrate | enqueue | stats | reap | prune`.

The connection string comes from `--dsn` or the PGTASKQ_DSN environment variable.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import math
import os
import sys
from collections.abc import Sequence

import psycopg

from pg_taskq import jobs, outbox
from pg_taskq.retention import prune
from pg_taskq.schema import LATEST, migrate, schema_version

MAX_DELAY_S = 10 * 365 * 24 * 3600  # ten years; beyond that a delay is almost surely a mistake


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
    r = sub.add_parser("reap", help="requeue jobs whose lease expired")
    r.add_argument("--limit", type=int, default=100, help="at most this many jobs (default 100)")
    pr = sub.add_parser("prune", help="delete old finished jobs and published outbox messages")
    pr.add_argument(
        "--older-than",
        type=float,
        required=True,
        metavar="SECONDS",
        help="only rows finished/published more than this many seconds ago",
    )
    pr.add_argument("--states", default="done,dead", help="job states to prune (default done,dead)")
    pr.add_argument("--no-outbox", action="store_true", help="leave outbox messages alone")
    pr.add_argument("--batch", type=int, default=1000, help="rows per transaction (default 1000)")
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
            if not (math.isfinite(args.delay) and 0 <= args.delay <= MAX_DELAY_S):
                print(f"error: --delay must be between 0 and {MAX_DELAY_S} s", file=sys.stderr)
                return 2
            try:
                job_id = jobs.enqueue(
                    conn,
                    args.queue,
                    payload,
                    delay=dt.timedelta(seconds=args.delay),
                    priority=args.priority,
                    max_attempts=args.max_attempts,
                    idempotency_key=args.key,
                )
            except ValueError as exc:
                print(f"error: {exc}", file=sys.stderr)
                return 2
            print(job_id)
            return 0
        if args.cmd == "stats":
            print(json.dumps({"jobs": jobs.stats(conn), "outbox_pending": outbox.pending(conn)}))
            return 0
        if args.cmd == "reap":
            if args.limit < 1:
                print("error: --limit must be >= 1", file=sys.stderr)
                return 2
            reaped = jobs.reap(conn, limit=args.limit)
            print(json.dumps({"requeued": reaped.requeued, "dead": reaped.dead}))
            return 0
        if args.cmd == "prune":
            if not (math.isfinite(args.older_than) and 0 <= args.older_than <= MAX_DELAY_S):
                print(f"error: --older-than must be between 0 and {MAX_DELAY_S} s", file=sys.stderr)
                return 2
            try:
                pruned = prune(
                    conn,
                    dt.timedelta(seconds=args.older_than),
                    states=[s.strip() for s in args.states.split(",") if s.strip()],
                    outbox=not args.no_outbox,
                    batch=args.batch,
                )
            except ValueError as exc:
                print(f"error: {exc}", file=sys.stderr)
                return 2
            print(json.dumps({"jobs": pruned.jobs, "outbox": pruned.outbox}))
            return 0
    return 2  # pragma: no cover - argparse rejects unknown commands


if __name__ == "__main__":
    sys.exit(main())
