from __future__ import annotations

import datetime as dt
import threading
import time
from collections.abc import Callable, Iterator
from typing import Any

import psycopg
import pytest

from pg_taskq import Backoff, Job, Worker, claim, enqueue, reap

Conn = psycopg.Connection[Any]
S = dt.timedelta(seconds=1)
FAST = Backoff(base=dt.timedelta(milliseconds=10), cap=dt.timedelta(milliseconds=20))


@pytest.fixture
def effects(conn: Conn) -> Iterator[None]:
    conn.execute("DROP TABLE IF EXISTS test_effects")
    conn.execute("CREATE TABLE test_effects (job_id bigint NOT NULL)")
    yield
    conn.execute("DROP TABLE IF EXISTS test_effects")


def count_effects(conn: Conn) -> int:
    row = conn.execute("SELECT count(*) FROM test_effects").fetchone()
    return int(row[0]) if row else -1


def job_state(conn: Conn, job_id: int) -> str:
    row = conn.execute("SELECT state FROM taskq_jobs WHERE id = %s", (job_id,)).fetchone()
    assert row is not None
    return str(row[0])


def write_effect(job: Job, conn: Conn) -> None:
    conn.execute("INSERT INTO test_effects VALUES (%s)", (job.id,))


def test_worker_processes_until_empty(
    connect: Callable[[], Conn], conn: Conn, effects: None
) -> None:
    ids = [enqueue(conn, "q", {"n": n}) for n in range(10)]
    stats = Worker(connect, "q", write_effect, worker_id="w1").run(until_empty=True)
    assert stats.completed == 10
    assert count_effects(conn) == 10
    assert {job_state(conn, i) for i in ids} == {"done"}


def test_failing_handler_retries_then_dies_without_effects(
    connect: Callable[[], Conn], conn: Conn, effects: None
) -> None:
    job_id = enqueue(conn, "q", max_attempts=2)

    def broken(job: Job, c: Conn) -> None:
        write_effect(job, c)
        raise ValueError("cannot render")

    worker = Worker(connect, "q", broken, worker_id="w1", poll_interval=0.01, backoff=FAST)
    stats = worker.run(max_jobs=2)
    assert (stats.retried, stats.dead, stats.completed) == (1, 1, 0)
    assert job_state(conn, job_id) == "dead"
    assert count_effects(conn) == 0  # each failed attempt rolled its writes back
    row = conn.execute("SELECT last_error FROM taskq_jobs WHERE id = %s", (job_id,)).fetchone()
    assert row == ("ValueError: cannot render",)


def test_heartbeats_keep_a_long_job_alive(
    connect: Callable[[], Conn], conn: Conn, effects: None
) -> None:
    job_id = enqueue(conn, "q")
    reaped: list[int] = []
    running = threading.Event()

    def slow(job: Job, c: Conn) -> None:
        running.set()
        time.sleep(1.6)  # longer than the 1 s lease
        write_effect(job, c)

    worker = Worker(connect, "q", slow, worker_id="w1", lease=S, heartbeat_every=0.2)
    t = threading.Thread(target=lambda: worker.run(max_jobs=1))
    t.start()
    assert running.wait(5)
    deadline = time.monotonic() + 1.4
    while time.monotonic() < deadline:  # a reaper running the whole time finds nothing
        reaped += reap(conn).requeued
        time.sleep(0.05)
    t.join()
    assert reaped == []
    assert worker.stats.completed == 1
    assert job_state(conn, job_id) == "done"
    assert count_effects(conn) == 1


def test_lost_lease_rolls_back_the_handlers_writes(
    connect: Callable[[], Conn], conn: Conn, effects: None
) -> None:
    job_id = enqueue(conn, "q")
    other = connect()

    def handler(job: Job, c: Conn) -> None:
        # Meanwhile the lease is declared expired and the job requeued (a long GC pause,
        # a network partition...). Simulated with a reaper whose clock is an hour ahead.
        reap(other, now=dt.datetime.now(dt.UTC) + 3600 * S)
        write_effect(job, c)

    stats = Worker(connect, "q", handler, worker_id="w1").run(max_jobs=1)
    assert (stats.lost, stats.completed) == (1, 0)
    assert count_effects(conn) == 0
    assert job_state(conn, job_id) == "queued"  # someone else will run it


def test_stop_finishes_the_current_job(
    connect: Callable[[], Conn], conn: Conn, effects: None
) -> None:
    for _ in range(3):
        enqueue(conn, "q")
    worker: Worker

    def handler(job: Job, c: Conn) -> None:
        worker.stop()
        write_effect(job, c)

    worker = Worker(connect, "q", handler, worker_id="w1")
    assert worker.run().completed == 1
    assert count_effects(conn) == 1


def test_worker_reaps_a_crashed_workers_job(
    connect: Callable[[], Conn], conn: Conn, effects: None
) -> None:
    job_id = enqueue(conn, "q")
    assert claim(conn, "q", "crashed", lease=dt.timedelta(milliseconds=300))  # never finished
    time.sleep(0.4)
    stats = Worker(connect, "q", write_effect, worker_id="w2", reap_every=0.0).run(until_empty=True)
    assert stats.completed == 1
    assert job_state(conn, job_id) == "done"


def test_heartbeat_interval_must_fit_the_lease(connect: Callable[[], Conn]) -> None:
    with pytest.raises(ValueError, match="heartbeat_every"):
        Worker(connect, "q", write_effect, worker_id="w", lease=S, heartbeat_every=1.0)
