"""Property-based test: random sequences of queue operations by several workers, including
crashes (a worker forgets its jobs) and clock jumps that expire leases, checked after every
step against a model of who owns what.

Invariants:
  I1  claim never returns a job that someone still owns (no job is owned by two workers);
  I2  an operation with a stale lease token is rejected and changes nothing;
  I3  attempts never exceed max_attempts; queued jobs are still eligible (attempts < max);
      running <=> lease set; done/dead <=> finished_at set;
  I4  the reaper takes back exactly the expired leases;
  I5  after the sequence, draining the queue ends every job done or dead.
"""

from __future__ import annotations

import datetime as dt
import os
import random
from typing import Any

import psycopg
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st
from psycopg.rows import dict_row

from pg_taskq import Backoff, FailOutcome, Job, claim, complete, enqueue, fail, heartbeat, reap

from .conftest import T0

Conn = psycopg.Connection[Any]
WORKERS = ["w0", "w1"]
EXAMPLES = int(os.environ.get("PGTASKQ_PROPERTY_EXAMPLES", "100"))

worker = st.sampled_from(WORKERS)
claim_op = st.tuples(st.just("claim"), worker, st.integers(1, 3), st.integers(5, 30))
op = st.one_of(
    st.tuples(st.just("enqueue"), st.integers(1, 3), st.integers(0, 5)),
    claim_op,
    claim_op,  # listed twice: claims are what create contention, so draw them more often
    st.tuples(st.just("heartbeat"), worker, st.integers(0, 5), st.integers(5, 30)),
    st.tuples(st.just("complete"), worker, st.integers(0, 5)),
    st.tuples(st.just("fail"), worker, st.integers(0, 5), st.booleans()),
    st.tuples(st.just("crash"), worker),
    st.tuples(st.just("tick"), st.integers(0, 40)),
    st.tuples(st.just("reap")),
    st.tuples(st.just("expire")),  # jump past every lease, then reap
)


class Model:
    def __init__(self) -> None:
        self.now = T0
        self.held: dict[str, dict[int, Job]] = {w: {} for w in WORKERS}
        self.owner: dict[int, tuple[str, Any]] = {}  # job id -> (worker, lease token)
        self.lease_until: dict[int, dt.datetime] = {}
        self.ids: list[int] = []

    def pick(self, w: str, idx: int) -> Job | None:
        jobs = sorted(self.held[w])
        return self.held[w][jobs[idx % len(jobs)]] if jobs else None

    def owns(self, w: str, job: Job) -> bool:
        return self.owner.get(job.id) == (w, job.lease_token)

    def release(self, w: str, job: Job) -> None:
        self.held[w].pop(job.id, None)
        if self.owns(w, job):
            del self.owner[job.id]
            del self.lease_until[job.id]


def check_rows(conn: Conn, m: Model) -> None:
    with conn.cursor(row_factory=dict_row) as cur:
        rows = cur.execute("SELECT * FROM taskq_jobs").fetchall()
    assert sorted(r["id"] for r in rows) == sorted(m.ids)
    running = {}
    for r in rows:
        assert 0 <= r["attempts"] <= r["max_attempts"]  # I3
        assert r["state"] in {"queued", "running", "done", "dead"}
        if r["state"] == "queued":
            assert r["attempts"] < r["max_attempts"]
        assert (r["state"] == "running") == (r["lease_token"] is not None)
        assert (r["state"] in {"done", "dead"}) == (r["finished_at"] is not None)
        if r["state"] == "running":
            running[r["id"]] = r["lease_token"]
    # The database's leases are exactly the model's owners, token for token.
    assert running == {jid: token for jid, (_, token) in m.owner.items()}


def step(conn: Conn, m: Model, operation: tuple[Any, ...], backoff: Backoff) -> None:
    kind = operation[0]
    if kind == "enqueue":
        _, max_attempts, delay = operation
        m.ids.append(enqueue(conn, "p", max_attempts=max_attempts, delay=delay * S, now=m.now))
    elif kind == "claim":
        _, w, limit, lease = operation
        for job in claim(conn, "p", w, limit=limit, lease=lease * S, now=m.now):
            assert job.id not in m.owner, "I1: claimed a job someone still owns"
            m.owner[job.id] = (w, job.lease_token)
            m.lease_until[job.id] = m.now + lease * S
            m.held[w][job.id] = job
    elif kind == "heartbeat":
        _, w, idx, lease = operation
        if (job := m.pick(w, idx)) is not None:
            expected = m.owns(w, job)
            assert heartbeat(conn, job, lease=lease * S, now=m.now) == expected  # I2
            if expected:
                m.lease_until[job.id] = m.now + lease * S
            else:
                m.held[w].pop(job.id)
    elif kind == "complete":
        _, w, idx = operation
        if (job := m.pick(w, idx)) is not None:
            assert complete(conn, job, now=m.now) == m.owns(w, job)  # I2
            m.release(w, job)
    elif kind == "fail":
        _, w, idx, retry = operation
        if (job := m.pick(w, idx)) is not None:
            outcome = fail(conn, job, "err", retry=retry, backoff=backoff, now=m.now)
            assert (outcome is not FailOutcome.LOST) == m.owns(w, job)  # I2
            if outcome is FailOutcome.RETRY:
                assert retry
                assert job.attempts < job.max_attempts
            m.release(w, job)
    elif kind == "crash":
        m.held[operation[1]].clear()  # the process is gone; its leases will expire
    elif kind == "tick":
        m.now += operation[1] * S
    elif kind in {"reap", "expire"}:
        if kind == "expire":
            m.now += 31 * S
        expired = {jid for jid, until in m.lease_until.items() if until < m.now}
        reaped = reap(conn, now=m.now)
        assert set(reaped.requeued) | set(reaped.dead) == expired  # I4
        for jid in expired:
            del m.owner[jid]
            del m.lease_until[jid]


S = dt.timedelta(seconds=1)


@settings(
    max_examples=EXAMPLES,
    deadline=None,
    suppress_health_check=[HealthCheck.function_scoped_fixture, HealthCheck.too_slow],
)
@given(ops=st.lists(op, min_size=10, max_size=60), seed=st.integers(0, 2**32 - 1))
def test_random_operation_sequences(conn: Conn, ops: list[Any], seed: int) -> None:
    conn.execute("TRUNCATE taskq_jobs RESTART IDENTITY")
    backoff = Backoff(rng=random.Random(seed))
    m = Model()
    for operation in ops:
        step(conn, m, operation, backoff)
        check_rows(conn, m)
    # I5: drain. Jump past every lease and backoff, reap, and finish whatever is claimable.
    for _ in range(10):
        m.now += dt.timedelta(days=1)
        reap(conn, now=m.now)
        m.owner.clear()
        m.lease_until.clear()
        batch = claim(conn, "p", "drainer", limit=1000, now=m.now)
        if not batch:
            break
        for job in batch:
            assert complete(conn, job, now=m.now)
    states = {r[0] for r in conn.execute("SELECT DISTINCT state FROM taskq_jobs").fetchall()}
    assert states <= {"done", "dead"}
