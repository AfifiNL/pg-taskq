"""Model-based property test: random sequences of queue operations by two workers, including
crashes (a worker forgets its jobs) and clock jumps that expire leases, checked after every step
against a model of every job's state, attempts, due time and lease owner.

Invariants:
  I1  claim returns exactly the jobs the model says are due, in priority/age/id order, and
      never one that someone still owns (no job is owned by two workers);
  I2  an operation with a stale lease token is rejected and changes nothing;
  I3  states, attempts and leases in the database match the model after every step;
      attempts never exceed max_attempts; queued jobs are still eligible;
  I4  the reaper takes back exactly the expired leases, requeueing or killing as the model says;
  I5  after the sequence, draining the queue ends every job done or dead.

Explicit @example sequences pin the "claim, lease expires, reaped, re-claimed by someone else,
stale worker acts" chain, which random generation reaches only now and then.
"""

from __future__ import annotations

import datetime as dt
import os
import random
from dataclasses import dataclass
from typing import Any
from uuid import UUID

import psycopg
from hypothesis import HealthCheck, example, given, settings
from hypothesis import strategies as st
from psycopg.rows import dict_row

from pg_taskq import Backoff, FailOutcome, Job, claim, complete, enqueue, fail, heartbeat, reap

from .conftest import T0

Conn = psycopg.Connection[Any]
S = dt.timedelta(seconds=1)
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

# claim by w0, lease expires, reaped, re-claimed by w1, then the stale w0 acts.
STALE_CHAIN = [
    ("enqueue", 3, 0),
    ("claim", "w0", 1, 5),
    ("expire",),
    ("claim", "w1", 1, 30),
]
STALE_COMPLETE = [*STALE_CHAIN, ("complete", "w0", 0), ("complete", "w1", 0)]
STALE_HEARTBEAT = [*STALE_CHAIN, ("heartbeat", "w0", 0, 30), ("complete", "w1", 0)]
STALE_FAIL = [*STALE_CHAIN, ("fail", "w0", 0, True), ("fail", "w1", 0, True)]
LAST_ATTEMPT = [("enqueue", 1, 0), ("claim", "w0", 1, 5), ("expire",), ("claim", "w1", 1, 5)]


@dataclass
class ModelJob:
    state: str
    attempts: int
    max_attempts: int
    run_at: dt.datetime
    owner: tuple[str, UUID] | None = None
    lease_until: dt.datetime | None = None


class Model:
    def __init__(self) -> None:
        self.now = T0
        self.jobs: dict[int, ModelJob] = {}
        self.held: dict[str, dict[int, Job]] = {w: {} for w in WORKERS}

    def pick(self, w: str, idx: int) -> Job | None:
        ids = sorted(self.held[w])
        return self.held[w][ids[idx % len(ids)]] if ids else None

    def owns(self, w: str, job: Job) -> bool:
        return self.jobs[job.id].owner == (w, job.lease_token)

    def due(self) -> list[int]:
        ready = [
            (m.run_at, jid)
            for jid, m in self.jobs.items()
            if m.state == "queued" and m.run_at <= self.now and m.attempts < m.max_attempts
        ]
        return [jid for _, jid in sorted(ready)]  # all priorities are 0 here

    def finish(self, jid: int, state: str) -> None:
        m = self.jobs[jid]
        m.state, m.owner, m.lease_until = state, None, None


def check_rows(conn: Conn, m: Model) -> None:
    with conn.cursor(row_factory=dict_row) as cur:
        rows = {r["id"]: r for r in cur.execute("SELECT * FROM taskq_jobs").fetchall()}
    assert sorted(rows) == sorted(m.jobs)
    for jid, r in rows.items():
        mj = m.jobs[jid]
        assert (r["state"], r["attempts"]) == (mj.state, mj.attempts)  # I3
        assert 0 <= r["attempts"] <= r["max_attempts"]
        if r["state"] == "queued":
            assert r["attempts"] < r["max_attempts"]
            assert r["run_at"] == mj.run_at
        assert (r["state"] == "running") == (r["lease_token"] is not None)
        assert (r["state"] in {"done", "dead"}) == (r["finished_at"] is not None)
        if r["state"] == "running":
            assert mj.owner is not None
            assert r["lease_token"] == mj.owner[1]
            assert r["locked_until"] == mj.lease_until


def step(conn: Conn, m: Model, operation: tuple[Any, ...], backoff: Backoff) -> None:
    kind = operation[0]
    if kind == "enqueue":
        _, max_attempts, delay = operation
        jid = enqueue(conn, "p", max_attempts=max_attempts, delay=delay * S, now=m.now)
        m.jobs[jid] = ModelJob("queued", 0, max_attempts, m.now + delay * S)
    elif kind == "claim":
        _, w, limit, lease = operation
        expected = m.due()[:limit]
        got = claim(conn, "p", w, limit=limit, lease=lease * S, now=m.now)
        assert [j.id for j in got] == expected  # I1: exactly the due jobs, in order
        for job in got:
            mj = m.jobs[job.id]
            assert mj.owner is None, "I1: claimed a job someone still owns"
            mj.attempts += 1
            assert job.attempts == mj.attempts
            mj.state, mj.owner, mj.lease_until = "running", (w, job.lease_token), m.now + lease * S
            m.held[w][job.id] = job
    elif kind == "heartbeat":
        _, w, idx, lease = operation
        if (job := m.pick(w, idx)) is not None:
            expected = m.owns(w, job)
            assert heartbeat(conn, job, lease=lease * S, now=m.now) == expected  # I2
            if expected:
                m.jobs[job.id].lease_until = m.now + lease * S
            else:
                m.held[w].pop(job.id)
    elif kind == "complete":
        _, w, idx = operation
        if (job := m.pick(w, idx)) is not None:
            expected = m.owns(w, job)
            assert complete(conn, job, now=m.now) == expected  # I2
            m.held[w].pop(job.id)
            if expected:
                m.finish(job.id, "done")
    elif kind == "fail":
        _, w, idx, retry = operation
        if (job := m.pick(w, idx)) is not None:
            mj = m.jobs[job.id]
            outcome = fail(conn, job, "err", retry=retry, backoff=backoff, now=m.now)
            m.held[w].pop(job.id)
            if not m.owns(w, job):
                assert outcome is FailOutcome.LOST  # I2
                return
            if retry and mj.attempts < mj.max_attempts:
                assert outcome is FailOutcome.RETRY
                m.finish(job.id, "queued")
                row = conn.execute("SELECT run_at FROM taskq_jobs WHERE id = %s", (job.id,))
                run_at = row.fetchone()[0]  # type: ignore[index]
                full = min(backoff.cap, backoff.base * 2 ** (mj.attempts - 1))
                assert m.now + full / 2 <= run_at <= m.now + full
                mj.run_at = run_at
            else:
                assert outcome is FailOutcome.DEAD
                m.finish(job.id, "dead")
    elif kind == "crash":
        m.held[operation[1]].clear()  # the process is gone; its leases will expire
    elif kind == "tick":
        m.now += operation[1] * S
    elif kind in {"reap", "expire"}:
        if kind == "expire":
            m.now += 31 * S
        expired = {
            jid
            for jid, mj in m.jobs.items()
            if mj.state == "running" and mj.lease_until is not None and mj.lease_until < m.now
        }
        reaped = reap(conn, now=m.now)
        assert set(reaped.requeued) | set(reaped.dead) == expired  # I4
        for jid in expired:
            mj = m.jobs[jid]
            if mj.attempts < mj.max_attempts:
                assert jid in reaped.requeued
                m.finish(jid, "queued")
                mj.run_at = m.now
            else:
                assert jid in reaped.dead
                m.finish(jid, "dead")


@settings(
    max_examples=EXAMPLES,
    deadline=None,
    suppress_health_check=[HealthCheck.function_scoped_fixture, HealthCheck.too_slow],
)
@given(ops=st.lists(op, min_size=10, max_size=60), seed=st.integers(0, 2**32 - 1))
@example(ops=STALE_COMPLETE, seed=0)
@example(ops=STALE_HEARTBEAT, seed=0)
@example(ops=STALE_FAIL, seed=0)
@example(ops=LAST_ATTEMPT, seed=0)
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
        batch = claim(conn, "p", "drainer", limit=1000, now=m.now)
        if not batch:
            break
        for job in batch:
            assert complete(conn, job, now=m.now)
    states = {r[0] for r in conn.execute("SELECT DISTINCT state FROM taskq_jobs").fetchall()}
    assert states <= {"done", "dead"}
