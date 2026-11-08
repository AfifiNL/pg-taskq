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

**Connection loss is a normal event.** The worker keeps its loop/handler, heartbeat and LISTEN
connections behind a small wrapper that re-opens a closed or broken connection on next use (the
LISTEN connection re-issues its LISTEN). A failed heartbeat drops the connection and the next beat reconnects (the lease has slack:
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
finished. The window ends when `prune()` deletes the row: after that the same key creates a new
job. Choose the retention period with that in mind.

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

**Wake-ups: LISTEN/NOTIFY, opt-in, with polling as the safety net.** With
`enqueue(..., notify=True)`, a job that is due at once calls `pg_notify()` on its queue's channel
in the same transaction; PostgreSQL delivers it at commit, and only then, so a rolled-back enqueue
wakes nobody, and several enqueues in one transaction fold into one notification. The reaper
notifies the queues of the jobs it requeues. Delayed jobs and retries (whose `run_at` is in the
future) send nothing: nobody could take them yet. A `Worker(listen=True)` keeps a third
connection that only LISTENs; when a claim comes back empty it waits on that connection for up to
`poll_interval`, in 0.25 s slices so `stop()` stays prompt. A notification only shortens the
wait: if one is lost, the next poll finds the job. Polling is the correctness mechanism;
notifications are a latency optimisation.

*Why opt-in.* The first version notified on every enqueue by default. A review measured the cost
and found two failure modes, all documented PostgreSQL behaviour:
- PostgreSQL serialises the commits of transactions that issued NOTIFY, database-wide (a global
  lock on the notification queue at commit). `make bench` measures both paths with 8 producers,
  one transaction per enqueue; the README's table has the result for one machine.
- A transaction that issued NOTIFY cannot be prepared for two-phase commit (PREPARE TRANSACTION
  fails), so a default NOTIFY would break callers that enqueue inside 2PC.
- Notifications wait in a server-wide queue (8 GB by default) until every listening session has
  read them. A listener that stops reading holds the queue back; when it fills, every commit that
  NOTIFYs fails. Workers therefore drain their LISTEN connection after every job as well as when
  idle, but one very long job still leaves it unread for that long.
Off by default, the queue pays none of these costs unless asked; turning it on is two arguments.

- The LISTEN is (re-)established *before* each claim. A job committed between an empty claim and
  the wait therefore leaves a notification waiting, and the wait returns at once (tested by
  injecting an enqueue right after an empty claim, with a 30 s poll interval).
- Channels are per database, tables are per schema. The channel is `taskq_` plus 24 hex
  characters of SHA-256 over the table schema (`current_schema()`) and the queue name, so two
  schemas in one database never wake each other (tested), and any name maps to a short, valid,
  lowercase identifier. It is computed in SQL by both `enqueue()` and the worker, so producers
  need no extra round trip; `pg_notify()` takes it as a bound value and LISTEN uses
  `sql.Identifier`, so nothing is spliced into SQL.
- The LISTEN connection gets TCP keepalives (30 s idle, 10 s interval, 3 probes) and a 60 s TCP
  user timeout, and every `listen_check_every` seconds (60 by default) the worker re-issues its
  LISTEN, a no-op round trip that proves the connection alive; a half-open connection then fails
  within about a minute instead of waiting silently. A broken LISTEN connection is re-opened and
  re-LISTENed at once, with backoff, not after the poll interval. If LISTEN keeps failing, the
  worker logs once and keeps claiming by polling. A connection whose LISTEN failed is closed.
- Poolers: under transaction pooling (PgBouncer `pool_mode = transaction`, for example) LISTEN
  runs on whichever server connection served that statement; that server connection is then
  handed to other clients, so notifications are lost or arrive elsewhere. Give `listen=True`
  workers a direct or session-mode connection, or leave wake-ups off.
- psycopg buffers notifications only on connections that LISTEN. Producer connections never
  LISTEN, so they never accumulate notifications.
- A bug found while building this: psycopg's `notifies()` generator holds the connection's lock
  while it is suspended, so draining extra notifications from inside the loop over the first one
  deadlocked the worker. The code closes the first generator before draining.

**Retention in small batches.** `prune()` deletes `done`/`dead` jobs and published outbox messages
older than a cut-off, in batches (`LIMIT` + `FOR UPDATE SKIP LOCKED`, 1,000 rows by default), each
batch its own short transaction. It never waits on rows someone else holds (tested with a row
held `FOR UPDATE` by another connection and a 2 s statement timeout) and never holds many locks
itself, so it can run next to busy workers (tested: four workers drain 600 jobs while a pruner
deletes them as they finish; every job is processed once and deleted once). Queued and running
jobs are never eligible; passing such a state is a `ValueError`. Pruning a job frees its
idempotency key, and `enqueue()` handles the race where a prune deletes the conflicting row
between its `INSERT … ON CONFLICT DO NOTHING` and the lookup of the existing id: it retries the
insert (up to 3 attempts) instead of failing.

**Migrations that touch busy tables.** Migration 2 adds the two partial indexes `prune()` uses.
A review reproduced a deadlock in the first version: `migrate()` held SHARE on `taskq_jobs` (its
first index build) and waited for `taskq_outbox`, while the README's own handler held a write lock
on `taskq_outbox` (`outbox.add`) and waited for `taskq_jobs` (`complete`). Now:
- before its DDL, a migration locks the tables it changes, waiting (bounded by `lock_timeout`,
  1 s by default) only for the first while it holds no lock a job transaction could need, and
  taking the rest with `NOWAIT`; it therefore never waits while holding a lock, and cannot be in a
  deadlock with application transactions that touch the tables in either order (both orders are
  tested against an open application transaction);
- a timeout or a `NOWAIT` miss rolls the attempt back and retries with backoff (30 attempts by
  default); after the last, `MigrationLockError` says what to do;
- the index builds still block writes while they run. On large tables, build the indexes first
  with `CREATE INDEX CONCURRENTLY IF NOT EXISTS …` (README); the migration uses `IF NOT EXISTS`
  and then only checks that each index is valid, refusing to record the version on top of an
  INVALID index left by a failed concurrent build (tested);
- `migrate()` is meant for deploy time; calling it at every worker start is safe but takes these
  locks briefly each time a migration is pending.

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
- **What the property test does and does not catch, measured by mutation** (re-run on the
  final code, full suite, same result). Five bugs were planted one at a time (`claim()` may take running jobs; `complete()`, `heartbeat()` or `fail()`
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
- **Wake-ups:** a worker idling with a 30 s poll interval picks up a newly enqueued job in well
  under a second; a job enqueued with `notify=False` (a lost notification) is still found by the
  next poll; killing the LISTEN connection falls back to polling and re-LISTENs; notifications are
  sent on commit only, folded within a transaction, and not sent for delayed jobs or other queues.
- **Retention:** only old `done`/`dead` rows and published messages go; prune running next to four
  claiming workers, and four prunes in parallel, each delete every row exactly once; migrating a
  version-1 schema to version 2 adds the indexes.
- **The demo is a smoke test.** `make check` runs it and it exits non-zero unless 40 jobs end
  done, the poison job ends dead and all 40 outbox messages are published.

## Benchmark method

`bench/bench.py` (`make bench`) uses separate processes, not threads, so the numbers include no
GIL contention between workers or producers. A barrier releases all processes at once; each
connects after the barrier (the `Worker` class connects lazily, so the loop scenarios do the same
for comparable timing); wall time runs from the barrier to the last result, and a child that
dies fails the run at once instead of after a timeout.

- `enqueue` / `enqueue+notify`: 8 producers, one transaction per enqueue, without and with
  `notify=True`. This is the measured cost of the NOTIFY commit serialisation.
- `loop-1`, `loop-10`, `worker`: 8 processes drain 20,000 no-op jobs (enqueued in one
  transaction beforehand, not measured); claim latency is timed around each `claim()` call in the
  loop scenarios. The run fails unless every job id appears exactly once across processes and
  the database counts every job `done`. That is a sanity check of a clean run, not a
  fault-injection test.
- `pickup-poll` / `pickup-notify`: one idle `Worker` with a 1 s poll interval; 20 jobs are
  enqueued one at a time, 100 ms apart; the delay from the enqueue's commit to the handler's start
  is measured, without and with wake-ups.

Results and caveats are in the README.
