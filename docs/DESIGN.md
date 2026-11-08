# Design notes

## Goals, in priority order

1. A job is never run by two workers at once on purpose, and a worker that lost a job cannot
   change it any more.
2. A crashed, hung or partitioned worker never loses a job: its work is taken back and retried,
   and a job that keeps failing ends in a visible `dead` state instead of looping forever.
3. Plain PostgreSQL and plain SQL: no extension, no broker, no ORM, readable queries.
4. Every claim above is tested against a real PostgreSQL, including under concurrency.

## Decisions

**Leases, not long transactions.** A claim is a short committed transaction that marks the row
`running` with `locked_until`. The alternative, holding the row lock for the whole job, ties up a
connection and an open transaction per running job (blocking vacuum), and a crashed worker's
lock only disappears when its connection times out. A lease makes a stuck worker visible
(`locked_until` in the past) and recoverable by a reaper.

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

**Completion joins the handler's transaction.** The `Worker` runs the handler and `complete()`
in one transaction. If the lease was lost in the meantime, `complete()` matches no row and the
worker rolls back the handler's writes: database effects of a job happen once even when the
handler ran twice. Effects outside the database (e-mail, HTTP calls) are not covered and must be
idempotent; this is stated in the README.

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
Delivery is therefore at-least-once; consumers deduplicate by message id.

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
package locally (no Docker), a PostgreSQL 17 service container in CI.

- **Property-based test** (`tests/test_properties.py`, Hypothesis): random sequences of enqueue,
  claim, heartbeat, complete, fail, worker crash, clock jumps and reaps by two workers, checked
  after every step against a model of who owns which lease. Invariants: claim never returns a job
  someone still owns; stale tokens are rejected; attempts never exceed the maximum; queued jobs
  are still eligible; the reaper takes back exactly the expired leases; draining at the end
  leaves every job done or dead.
- **The property test catches real bugs.** Checked by mutation while writing it: allowing
  `claim()` to take `running` rows, and dropping the token check from `complete()`. The first
  version of the test missed both (its random sequences were too short and rarely built the
  "claim, expire, reap, re-claim, stale complete" chain); longer sequences, more claims and an
  "expire" step fixed that. Both mutations now fail it.
- **Concurrency test:** 8 threads with their own connections claim 400 jobs at once; no job is
  claimed twice.
- **Worker tests** with real time: heartbeats keep a 1.6 s job alive under a 1 s lease while a
  reaper runs; a lost lease rolls the handler's writes back; a crashed worker's job is reaped.
