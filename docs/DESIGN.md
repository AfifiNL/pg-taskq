# Design notes

## Goals, in priority order

1. A job is never run by two workers at once on purpose, and a worker that lost a job cannot
   change it any more.
2. A crashed, hung or partitioned worker never loses a job: its work is taken back and retried,
   and a job that keeps failing ends in a visible `dead` state instead of looping forever.
3. Plain PostgreSQL and plain SQL: no extension, no broker, no ORM, readable queries.
4. Every claim above is tested against a real PostgreSQL, including under concurrency.

## Decisions

**Leases, not row locks held for the job.** A claim is a short committed transaction that marks
the row `running` with `locked_until`. Ownership is that lease, not a lock: the alternative,
holding the job row locked for the whole job, ties up a connection and an open transaction per
running job, and a crashed worker's lock only disappears when its connection times out. A lease
makes a stuck worker visible (`locked_until` in the past) and recoverable by a reaper. (Whether
the *handler* keeps a transaction open is a separate choice; see "Two worker modes".)

**A token per claim, checked on every write.** Each claim sets a fresh random `lease_token`.
`heartbeat`, `complete` and `fail` only touch the row while it still carries that token, and
report `False` / `LOST` otherwise. A worker identity is not enough: a restarted process with the
same name must not be able to act on its predecessor's lease.

**Only the reaper takes a lease away.** `reap()` moves expired leases back to `queued` (or to
`dead` when that attempt was the last), with an error saying which worker stopped heartbeating.
`claim()` never takes running jobs, even expired ones, so every ownership change happens in one
place. Any process may reap; `FOR UPDATE SKIP LOCKED` keeps concurrent reapers apart, and each
`Worker` reaps every 30 s by default. Until a lease is reaped, its holder may still finish the
job: nobody else can own it in the meantime.

**Attempts are counted at claim time.** A job that kills its worker (out of memory, a segfault)
never reaches `fail()`. Counting the claim as the attempt means such a job still dies after
`max_attempts` instead of being retried forever.

**`FOR UPDATE SKIP LOCKED` for claims.** Concurrent claimers skip rows another claimer is
taking at that moment instead of queueing behind its lock, so N workers neither block each other
nor get the same row. Order within a queue: priority (high first), then `run_at`, then id.

**The database clock is the only clock.** Every time comparison uses `statement_timestamp()`,
so workers on different hosts never compare their own clocks. Each function takes an optional
`now=` so tests can move time without sleeping.

**Two worker modes, because "exactly-once writes" and "no long transactions" conflict.**
- `transactional=True` (default): the handler and `complete()` run in one transaction. If the
  lease was lost meanwhile, `complete()` matches no row and the worker rolls back the handler's
  writes, so a job's database effects happen once even when the handler ran twice. The cost is
  an open transaction for the whole job: it holds back vacuum's horizon, and a handler that
  spends long outside the database is cut off by `idle_in_transaction_session_timeout` where that
  is set. Fine for short jobs and jobs that are mostly database work.
- `transactional=False`: the handler gets an autocommit connection and commits its own short
  transactions; `complete()` runs afterwards in its own transaction, still fenced by the token.
  A lost lease is detected and counted, but writes already committed stay, so handlers must be
  idempotent. For long jobs.

The first version had only the default mode while this document said "leases, not long
transactions"; a review caught the contradiction. The option is the smallest change that makes
both properties available and the trade-off explicit. Effects outside the database (e-mail,
HTTP calls) are at-least-once in both modes.

**Connection loss is a normal event.** The worker keeps its loop/handler connection and its
heartbeat connection behind a small wrapper that re-opens a closed or broken connection on next
use. A failed heartbeat drops the connection and the next beat reconnects (the lease has slack:
beats run every lease/3 by default). A connection error while claiming or reaping is logged and
retried with backoff (200 ms doubling to 10 s); `run()` does not exit. If a handler's failure
cannot be recorded because the connection broke, the job is left to the reaper, which requeues
it when its lease expires, with that attempt counted. The heartbeat thread is joined with a
timeout, so a hung statement cannot hang the worker.

**Inputs are checked before they reach SQL.** Naive datetimes are rejected: PostgreSQL would read
them in the session's `TimeZone`, so the same call would mean different instants on differently
configured connections. Queue, worker and topic names must be non-empty and storable. Error text
is made storable before it is written: PostgreSQL `text` cannot hold NUL bytes, and lone
surrogates cannot be encoded as UTF-8; both are written as backslash escapes, capped at 2,000
characters. Without this, one exception message with a NUL byte made `fail()` raise inside the
worker (the job stayed `running`, `run()` died) and made the outbox relay roll back its batch on
every call, re-sending the messages before it forever. Priority and `max_attempts` are checked
against the 32-bit column range.

**Composable transactions.** Every function wraps its statements in `conn.transaction()`: on an
idle connection that is its own committed transaction; inside a caller's transaction it is a
savepoint that commits with the caller.

**Backoff: exponential with "equal jitter".** Retry n waits between d/2 and d, with
d = min(cap, base · 2^(n−1)) (defaults: base 5 s, cap 15 min). The fixed half guarantees spacing;
the random half spreads out jobs that failed together. Computed in float seconds, because
`timedelta` overflows long before a float does. The random generator is injectable for tests.

**Idempotency keys are unique per queue, for the life of the row.** A duplicate enqueue returns
the existing job's id whatever its state, so "enqueue once per order" holds even after the job
finished. There is no pruning yet (see limits), so the window is the table's lifetime.

**Transactional outbox.** `outbox.add()` inserts in the caller's transaction, so a message exists
if and only if the change it describes committed. It refuses to run on an autocommit connection
outside a transaction block, where it would commit on its own and defeat the pattern. The relay
locks a batch with `SKIP LOCKED`, publishes, and marks rows published in the same transaction:
parallel relays take disjoint batches, a failed publish gets a backoff delay without blocking
the rest of the batch, and a relay that dies mid-batch rolls back and the batch goes out again.
Delivery is therefore at-least-once; consumers deduplicate by message id. There is no
delivery-order guarantee: a batch goes out in id order, but ids are assigned at insert, not at
commit, a failed message waits for its retry while later ones go out, and parallel relays
interleave. A per-key ordering would need the relay to stop at a key's first failure and a
single relay per key; that is not built.

**Polling, not LISTEN/NOTIFY.** Idle workers poll every `poll_interval` (1 s by default). It is
simpler and has no lost-notification edge cases; the cost is up to one poll interval of latency.

**Schema.** Fixed table names (`taskq_jobs`, `taskq_outbox`, `taskq_schema_version`) in the
connection's `search_path`. Numbered migrations run in one transaction under a transaction-scoped
advisory lock, so two processes starting together cannot both apply one. Check constraints
encode the state machine: `running` ⇔ lease set, `done`/`dead` ⇔ `finished_at` set,
`0 ≤ attempts ≤ max_attempts`.

## Testing

`SKIP LOCKED`, row locks and transaction boundaries cannot be mocked meaningfully, so every
queue test runs against a real PostgreSQL: a throwaway server from the pip-installed `pgserver`
package locally (no Docker), a PostgreSQL 17 service container in CI. Each test session creates
its own schema and drops it afterwards, so pointing `PGTASKQ_TEST_DATABASE_URL` at a database
with real taskq tables cannot touch them. Assertions in helper threads are collected and
re-raised in the test (a failing assertion in a plain thread only produces a warning).

- **Model-based property test** (`tests/test_properties.py`, Hypothesis): random sequences of
  enqueue, claim, heartbeat, complete, fail, worker crash, clock jumps and reaps by two workers.
  After every step, every job's state, attempts, due time, lease token and lease expiry in the
  database must equal the model's; claims must return exactly the jobs the model says are due,
  in order; stale tokens must be rejected; the reaper must take exactly the expired leases;
  draining at the end must leave every job done or dead. Pinned `@example`s cover the chain
  "claim, lease expires, reaped, re-claimed by another worker, the stale worker completes /
  heartbeats / fails" and a last attempt expiring.
- **What the property test does and does not catch, measured by mutation.** Five bugs were
  planted one at a time (`claim()` may take running jobs; `complete()`, `heartbeat()` or `fail()`
  ignore the lease token; `reap()` without `SKIP LOCKED`). The property test fails on the first
  four. It cannot see the fifth: it runs on one connection, where `SKIP LOCKED` changes nothing.
  That one is caught only by `test_an_open_reap_does_not_block_another`, which holds one reap
  open and requires a second to finish without waiting.
- **Correction.** An earlier version of this document said the property test caught a
  `complete()` that ignores the token. A review showed it did not do so reliably: the random
  sequences rarely built the five-step chain that exposes it (it was caught in one local run and
  missed in the reviewer's). The pinned examples now make that deterministic, and the model now
  tracks attempts and outcomes instead of only lease owners.
- **Races on separate connections:** 8 connections claim 400 jobs at once (no job claimed
  twice); 4 reapers take 300 expired jobs (each exactly once); a heartbeat thread races a reaper
  over 200 jobs (for each job exactly one side wins, and the row agrees).
- **Worker tests** with real time: heartbeats keep a 1.6 s job alive under a 1 s lease while a
  reaper runs; killing the heartbeat connection (`pg_terminate_backend`) mid-job is survived and
  the job still completes; killing the idle main connection is survived; NUL and lone-surrogate
  error text is stored escaped and the worker carries on; both worker modes are tested against a
  lost lease.
- **The demo is a smoke test.** `make check` runs it and it exits non-zero unless 40 jobs end
  done, the poison job ends dead and all 40 outbox messages are published.
