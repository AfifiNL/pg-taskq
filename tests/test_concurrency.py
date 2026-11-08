"""Real concurrency: several connections claim from one queue at the same time."""

from __future__ import annotations

import functools
import threading
from collections import Counter
from collections.abc import Callable
from typing import Any

import psycopg

from pg_taskq import claim, complete, enqueue, stats

from .conftest import run_threads

Conn = psycopg.Connection[Any]
JOBS = 400
WORKERS = 8


def test_no_job_is_claimed_twice(connect: Callable[[], Conn], conn: Conn) -> None:
    for n in range(JOBS):
        enqueue(conn, "load", {"n": n})
    claimed: list[tuple[int, str]] = []
    lock = threading.Lock()
    start = threading.Barrier(WORKERS)

    def work(name: str) -> None:
        with connect() as c:
            start.wait()
            while batch := claim(c, "load", name, limit=3):
                for job in batch:
                    with lock:
                        claimed.append((job.id, name))
                    assert complete(c, job)

    run_threads(*[functools.partial(work, f"w{i}") for i in range(WORKERS)])
    counts = Counter(job_id for job_id, _ in claimed)
    assert len(counts) == JOBS
    assert max(counts.values()) == 1
    assert stats(conn) == {"load": {"done": JOBS}}
    # The claims really ran side by side (a weak check on purpose: scheduling varies).
    assert len({name for _, name in claimed}) >= 2
