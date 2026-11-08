"""Local benchmark: N worker processes drain M jobs from a throwaway PostgreSQL.

    make bench                                   # defaults: 8 workers, 20,000 jobs
    uv run --group localpg python bench/bench.py --workers 4 --jobs 5000

Scenarios:
  enqueue          N producer processes, one transaction per enqueue, the default (no NOTIFY)
  enqueue+notify   the same with notify=True (PostgreSQL serialises commits that NOTIFY)
  loop-1           N workers drain M no-op jobs: claim(limit=1) -> complete(); claim latency
  loop-10          the same with claim(limit=10)
  worker           the Worker class (heartbeat thread per job, handler + complete in one txn)
  pickup-poll      one idle Worker (poll_interval 1 s), jobs enqueued one by one: the delay from
                   commit to handler start, default path (polling)
  pickup-notify    the same with Worker(listen=True) and enqueue(notify=True)

The drain scenarios check that each job was processed exactly once (ids collected from all
processes, and the final counts in the database). That is a sanity check of a clean run, not a
fault-injection test. Numbers depend heavily on the machine and on PostgreSQL's settings (every
complete() is a commit, so the disk's flush latency dominates). Not part of `make check`.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import multiprocessing as mp
import os
import platform
import queue as queue_mod
import statistics
import tempfile
import threading
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
    start.wait()  # connect after the barrier, like the Worker scenario, so timing is comparable
    with psycopg.connect(dsn, autocommit=True) as conn:
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


def producer(dsn: str, queue: str, count: int, notify: bool, start: Any, out: Any) -> None:
    start.wait()
    with psycopg.connect(dsn, autocommit=True) as conn:
        for n in range(count):
            enqueue(conn, queue, {"n": n}, notify=notify)  # one transaction each
    out.put(([], []))


def collect(procs: list[Any], out: Any) -> list[Any]:
    """Results from every child; fail fast (not after a long timeout) if one of them dies."""
    results: list[Any] = []
    deadline = time.monotonic() + 600
    while len(results) < len(procs):
        try:
            results.append(out.get(timeout=0.5))
        except queue_mod.Empty:
            dead = [p.exitcode for p in procs if p.exitcode not in (None, 0)]
            if dead:
                raise RuntimeError(f"a child process failed (exit codes {dead})") from None
            if time.monotonic() > deadline:
                raise RuntimeError("children did not finish within 600 s") from None
    return results


def run_enqueue(dsn: str, name: str, producers: int, jobs: int, notify: bool) -> dict[str, Any]:
    ctx = mp.get_context("spawn")
    start = ctx.Barrier(producers + 1)
    out: Any = ctx.Queue()
    per = jobs // producers
    procs = [
        ctx.Process(target=producer, args=(dsn, f"bench_{name}", per, notify, start, out))
        for _ in range(producers)
    ]
    for p in procs:
        p.start()
    start.wait()
    t0 = time.perf_counter()
    collect(procs, out)
    elapsed = time.perf_counter() - t0
    for p in procs:
        p.join(timeout=60)
    return {
        "scenario": name,
        "workers": producers,
        "jobs": per * producers,
        "claim_limit": "",
        "seconds": round(elapsed, 2),
        "jobs_per_s": round(per * producers / elapsed),
    }


def run_pickup(dsn: str, name: str, jobs: int, notify: bool) -> dict[str, Any]:
    queue = f"bench_{name}"
    started: dict[int, float] = {}

    def handle(job: Job, conn: psycopg.Connection[Any]) -> None:
        started[job.id] = time.perf_counter()

    worker = Worker(
        lambda: psycopg.connect(dsn),
        queue,
        handle,
        worker_id="pickup",
        poll_interval=1.0,
        listen=notify,
    )
    thread = threading.Thread(target=worker.run)
    thread.start()
    committed: dict[int, float] = {}
    try:
        with psycopg.connect(dsn, autocommit=True) as conn:
            for n in range(jobs):
                time.sleep(0.1)  # let the worker go idle between jobs
                job_id = enqueue(conn, queue, {"n": n}, notify=notify)
                committed[job_id] = time.perf_counter()
                deadline = time.monotonic() + 10
                while job_id not in started:
                    if time.monotonic() > deadline:
                        raise RuntimeError(f"{name}: job {job_id} not picked up within 10 s")
                    time.sleep(0.001)
    finally:
        worker.stop()
        thread.join(timeout=10)
    delays = sorted(started[i] - committed[i] for i in committed)
    q = statistics.quantiles(delays, n=100)
    return {
        "scenario": name,
        "workers": 1,
        "jobs": jobs,
        "claim_limit": 1,
        "pickup_ms_p50": round(q[49] * 1000, 1),
        "pickup_ms_p95": round(q[94] * 1000, 1),
    }


def run_scenario(dsn: str, name: str, workers: int, jobs: int, limit: int) -> dict[str, Any]:
    queue = f"bench_{name}"
    with psycopg.connect(dsn, autocommit=True) as conn, conn.transaction():  # setup, unmeasured
        for n in range(jobs):
            enqueue(conn, queue, {"n": n})
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
    results = collect(procs, out)
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
    ap.add_argument("--pickups", type=int, default=20, help="jobs per pickup scenario")
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
                run_enqueue(dsn, "enqueue", args.workers, args.jobs, notify=False),
                run_enqueue(dsn, "enqueue+notify", args.workers, args.jobs, notify=True),
                run_scenario(dsn, "loop-1", args.workers, args.jobs, 1),
                run_scenario(dsn, "loop-10", args.workers, args.jobs, 10),
                run_scenario(dsn, "worker", args.workers, args.jobs, 1),
                run_pickup(dsn, "pickup-poll", args.pickups, notify=False),
                run_pickup(dsn, "pickup-notify", args.pickups, notify=True),
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
    print(
        "| scenario | processes | jobs | jobs/s | claim p50 ms | claim p95 ms "
        "| pickup p50 ms | pickup p95 ms |"
    )
    print("|---|---|---|---|---|---|---|---|")
    for r in rows:
        cells = [
            r.get(k, "n/a")
            for k in (
                "jobs_per_s",
                "claim_ms_p50",
                "claim_ms_p95",
                "pickup_ms_p50",
                "pickup_ms_p95",
            )
        ]
        rate = f"{cells[0]:,}" if isinstance(cells[0], int) else cells[0]
        print(
            f"| {r['scenario']} | {r['workers']} | {r['jobs']:,} | {rate} | "
            f"{cells[1]} | {cells[2]} | {cells[3]} | {cells[4]} |"
        )


if __name__ == "__main__":
    main()
