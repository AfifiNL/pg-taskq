"""End-to-end demo on a throwaway local PostgreSQL (no Docker, no credentials).

Three workers process 40 synthetic "render thumbnail" jobs. Every fourth job fails on its first
attempt (a transient error) and is retried with backoff; one poison job always fails and ends
dead; one job is claimed by a worker that then "crashes", and the others reap its lease. Each
successful job records its result and an outbox message in the same transaction as completing
the job; a relay then publishes the messages.
"""

from __future__ import annotations

import datetime as dt
import tempfile
import threading
import time
from pathlib import Path
from typing import Any

import pgserver  # type: ignore[import-untyped]
import psycopg

from pg_taskq import Backoff, Job, Worker, claim, enqueue, migrate, outbox, stats
from pg_taskq.outbox import OutboxMessage

QUEUE = "thumbnails"


def handler(job: Job, conn: psycopg.Connection[Any]) -> None:
    image = job.payload["image"]
    if job.payload.get("poison"):
        raise ValueError(f"image {image}: corrupt header")
    if image % 4 == 0 and job.attempts == 1:
        raise TimeoutError(f"image {image}: storage timed out")
    conn.execute("INSERT INTO thumbnails (image, size) VALUES (%s, %s)", (image, 128))
    outbox.add(conn, "thumbnail.ready", {"image": image}, key=str(image))


def main() -> None:
    with tempfile.TemporaryDirectory(prefix="pgtaskq-demo-") as tmp:
        server = pgserver.get_server(Path(tmp) / "pgdata", cleanup_mode="stop")
        dsn = server.get_uri()
        try:
            run(dsn)
        finally:
            server.cleanup()


def run(dsn: str) -> None:
    with psycopg.connect(dsn, autocommit=True) as conn:
        migrate(conn)
        conn.execute("CREATE TABLE thumbnails (image int PRIMARY KEY, size int NOT NULL)")
        for n in range(1, 41):
            enqueue(conn, QUEUE, {"image": n}, max_attempts=3)
        enqueue(conn, QUEUE, {"image": 99, "poison": True}, max_attempts=3)
        # A worker claims one job with a short lease and is never heard from again.
        [abandoned] = claim(conn, QUEUE, "crashed-worker", lease=dt.timedelta(seconds=1))
        print(f"crashed-worker took job {abandoned.id} and stopped")

        fast = Backoff(base=dt.timedelta(milliseconds=50), cap=dt.timedelta(milliseconds=200))
        workers = [
            Worker(
                lambda: psycopg.connect(dsn),
                QUEUE,
                handler,
                worker_id=f"worker-{i}",
                poll_interval=0.05,
                reap_every=0.2,
                backoff=fast,
            )
            for i in range(1, 4)
        ]
        started = time.monotonic()
        threads = [threading.Thread(target=w.run) for w in workers]
        for t in threads:
            t.start()
        while True:  # wait until nothing is queued or running
            counts = stats(conn).get(QUEUE, {})
            if not counts.get("queued") and not counts.get("running"):
                break
            time.sleep(0.05)
        for w in workers:
            w.stop()
        for t in threads:
            t.join()
        elapsed = time.monotonic() - started

        for w in workers:
            s = w.stats
            print(f"{w.worker_id}: completed={s.completed} retried={s.retried} dead={s.dead}")
        print(f"jobs: {stats(conn)[QUEUE]} in {elapsed:.1f} s")
        row = conn.execute("SELECT count(*) FROM thumbnails").fetchone()
        print(f"thumbnails written: {row[0] if row else 0}")
        dead = conn.execute(
            "SELECT payload->>'image', attempts, last_error FROM taskq_jobs WHERE state = 'dead'"
        ).fetchall()
        for image, attempts, error in dead:
            print(f"dead: image {image} after {attempts} attempts ({error})")

        published: list[OutboxMessage] = []
        while outbox.relay_once(conn, published.append, limit=25).published:
            pass
        print(f"outbox: published {len(published)} messages, {outbox.pending(conn)} pending")


if __name__ == "__main__":
    main()
