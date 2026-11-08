"""Local benchmark: N worker processes drain M jobs from a throwaway PostgreSQL.

    make bench                                   # defaults: 8 workers, 20,000 jobs
    uv run --group localpg python bench/bench.py --workers 4 --jobs 5000

Scenarios, each on its own queue of M no-op jobs:
  loop-1   claim(limit=1) -> complete(), the library primitives, per-claim latency measured
  loop-10  claim(limit=10) -> complete() each
  worker   the Worker class (heartbeat thread per job, handler + complete in one transaction)

Every scenario checks that each job was processed exactly once (ids collected from all
processes, and the database's final counts). Numbers depend heavily on the machine and on
PostgreSQL's settings (every complete() is a commit, so fsync latency dominates); the script
prints both. Not part of `make check`.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import multiprocessing as mp
import os
import platform
import statistics
import tempfile
import time
from pathlib import Path
from typing import Any

import psycopg

from pg_taskq import Job, Worker, claim, complete, enqueue, migrate, stats


def cpu_model() -> str:
    try:
        for line in Path("/proc/cpuinfo").read_text().splitlines():
            if line.startswith("model name"):
                return line.split(":", 1)[1].strip()
    except OSError:
        pass
    return platform.processor() or "unknown"


def loop_worker(dsn: str, queue: str, limit: int, start: Any, out: Any) -> None:
    ids: list[int] = []
    latencies: list[float] = []
    with psycopg.connect(dsn, autocommit=True) as conn:
        start.wait()
        while True:
            t0 = time.perf_counter()
            batch = claim(conn, queue, f"bench-{os.getpid()}", limit=limit)
            latencies.append(time.perf_counter() - t0)
            if not batch:
                break
            for job in batch:
                if not complete(conn, job):
                    raise RuntimeError(f"lost lease on job {job.id}")
                ids.append(job.id)
    out.put((ids, latencies))


def class_worker(dsn: str, queue: str, start: Any, out: Any) -> None:
    ids: list[int] = []

    def handle(job: Job, conn: psycopg.Connection[Any]) -> None:
        ids.append(job.id)

    worker = Worker(lambda: psycopg.connect(dsn), queue, handle, worker_id=f"bench-{os.getpid()}")
    start.wait()
    s = worker.run(until_empty=True)
    if s.lost or s.retried or s.dead:
        raise RuntimeError(f"unexpected worker stats {s}")
    out.put((ids, []))


def run_scenario(dsn: str, name: str, workers: int, jobs: int, limit: int) -> dict[str, Any]:
    queue = f"bench_{name}"
    with psycopg.connect(dsn, autocommit=True) as conn:
        t0 = time.perf_counter()
        with conn.transaction():
            for n in range(jobs):
                enqueue(conn, queue, {"n": n}, notify=False)
        enqueue_s = time.perf_counter() - t0
    ctx = mp.get_context("spawn")
    start = ctx.Barrier(workers + 1)
    out: Any = ctx.Queue()
    if name == "worker":
        procs = [
            ctx.Process(target=class_worker, args=(dsn, queue, start, out)) for _ in range(workers)
        ]
    else:
        procs = [
            ctx.Process(target=loop_worker, args=(dsn, queue, limit, start, out))
            for _ in range(workers)
        ]
    for p in procs:
        p.start()
    start.wait()
    t0 = time.perf_counter()
    results = [out.get(timeout=600) for _ in procs]
    elapsed = time.perf_counter() - t0
    for p in procs:
        p.join(timeout=60)
        if p.exitcode != 0:
            raise RuntimeError(f"a worker process exited with {p.exitcode}")
    ids = [i for r in results for i in r[0]]
    latencies = sorted(x for r in results for x in r[1])
    with psycopg.connect(dsn, autocommit=True) as conn:
        final = stats(conn).get(queue, {})
    once = len(ids) == len(set(ids)) == jobs and final == {"done": jobs}
    if not once:
        raise RuntimeError(f"{name}: {len(ids)} processed, {len(set(ids))} distinct, db {final}")
    row: dict[str, Any] = {
        "scenario": name,
        "workers": workers,
        "jobs": jobs,
        "claim_limit": limit,
        "seconds": round(elapsed, 2),
        "jobs_per_s": round(jobs / elapsed),
        "each_job_once": once,
        "enqueue_jobs_per_s_one_txn": round(jobs / enqueue_s),
    }
    if latencies:
        q = statistics.quantiles(latencies, n=100)
        row["claim_ms_p50"] = round(q[49] * 1000, 2)
        row["claim_ms_p95"] = round(q[94] * 1000, 2)
    return row


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--workers", type=int, default=8)
    ap.add_argument("--jobs", type=int, default=20_000)
    args = ap.parse_args()

    import pgserver  # type: ignore[import-untyped]  # local only: `make install`

    load_at_start = [round(x, 2) for x in os.getloadavg()]  # other work on the machine
    with tempfile.TemporaryDirectory(prefix="pgtaskq-bench-") as tmp:
        server = pgserver.get_server(Path(tmp) / "pgdata", cleanup_mode="stop")
        dsn = server.get_uri()
        try:
            with psycopg.connect(dsn, autocommit=True) as conn:
                migrate(conn)
                settings = {
                    k: conn.execute(f"SHOW {k}").fetchone()[0]  # type: ignore[index]
                    for k in ("server_version", "fsync", "synchronous_commit", "shared_buffers")
                }
            rows = [
                run_scenario(dsn, "loop-1", args.workers, args.jobs, 1),
                run_scenario(dsn, "loop-10", args.workers, args.jobs, 10),
                run_scenario(dsn, "worker", args.workers, args.jobs, 1),
            ]
        finally:
            server.cleanup()
    report = {
        "date": dt.datetime.now(dt.UTC).strftime("%Y-%m-%d %H:%M UTC"),
        "load_average_1_5_15_at_start": load_at_start,
        "cpu": cpu_model(),
        "logical_cpus": os.cpu_count(),
        "python": platform.python_version(),
        "psycopg": psycopg.__version__,
        "postgresql": settings,
        "results": rows,
    }
    print(json.dumps(report, indent=2))
    print()
    print("| scenario | workers | jobs | claim limit | jobs/s | claim p50 ms | claim p95 ms |")
    print("|---|---|---|---|---|---|---|")
    for r in rows:
        p50, p95 = r.get("claim_ms_p50", "n/a"), r.get("claim_ms_p95", "n/a")
        print(
            f"| {r['scenario']} | {r['workers']} | {r['jobs']:,} | {r['claim_limit']} | "
            f"{r['jobs_per_s']:,} | {p50} | {p95} |"
        )


if __name__ == "__main__":
    main()
