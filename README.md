# pg-taskq

Durable background jobs for Python on **plain PostgreSQL**: atomic claims with
`FOR UPDATE SKIP LOCKED`, leases kept alive by heartbeats, a reaper for crashed workers, retries
with exponential backoff and jitter, a dead state for jobs that keep failing, and a
**transactional outbox** for publishing events reliably. No broker, no extension, no ORM:
about 1,000 lines of typed Python (psycopg 3) and SQL, docstrings included. Needs PostgreSQL 13
or newer (`gen_random_uuid()`); tested on 16.2 and 17.

```python
# sketch: DSN is your connection string; invoices is your own table
import psycopg
from pg_taskq import Worker, enqueue, migrate, outbox

conn = psycopg.connect(DSN, autocommit=True)
migrate(conn)                                         # tables, idempotent
enqueue(conn, "invoices", {"order": 1042}, idempotency_key="order-1042")

def handle(job, conn):                                # runs in one transaction with complete()
    conn.execute("INSERT INTO invoices (order_id) VALUES (%s)", (job.payload["order"],))
    outbox.add(conn, "invoice.created", {"order": job.payload["order"]})

Worker(lambda: psycopg.connect(DSN), "invoices", handle, worker_id="host-a:1").run()
```

## Quick start (60 seconds, no Docker)

Requires Python 3.12+ and [uv](https://docs.astral.sh/uv/). The tests and the demo start a
throwaway PostgreSQL from the pip-installed [`pgserver`](https://pypi.org/project/pgserver/)
package, so nothing else is needed.

```bash
make install   # .venv with dev tools and the bundled PostgreSQL
make check     # ruff, mypy --strict, the tests against a real PostgreSQL, and the demo
make demo      # just the demo: 3 workers, retries, a crashed worker, a poison job, the outbox
```

As a dependency: `psycopg` needs libpq. Install the `binary` extra (`pg-taskq[binary]`, which
pulls in `psycopg[binary]`) unless your system already has libpq.

`make demo` printed this on the machine it was written on. Timings and the split of jobs between
the three workers vary from run to run; the totals do not, and the demo checks them itself:

```text
crashed-worker took job 1 and stopped
worker-1: completed=15 retried=4 dead=0
worker-2: completed=13 retried=3 dead=1
worker-3: completed=12 retried=5 dead=0
jobs: {'dead': 1, 'done': 40} in 1.1 s
thumbnails written: 40
dead: image 99 after 3 attempts (ValueError: image 99: corrupt header)
outbox: published 40 messages, 0 pending
demo: OK
```

What happened: 40 jobs plus one poison job; every fourth job failed once and was retried after a
backoff; the poison job failed 3 times and ended `dead`; the job a "crashed" worker had claimed
was reaped after its 1 s lease ran out and finished by another worker; every finished job wrote
its result and an outbox message in the same transaction; the relay published all 40.

With your own PostgreSQL, the CLI:

```bash
export PGTASKQ_DSN=postgresql://user@localhost:5432/app
uv run pg-taskq migrate
uv run pg-taskq enqueue reports '{"month": "2030-01"}' --key report-2030-01 --delay 60
uv run pg-taskq stats          # {"jobs": {"reports": {"queued": 1}}, "outbox_pending": 0}
uv run pg-taskq reap --limit 100   # requeue jobs whose lease expired
```

## How it works

```text
enqueue ──► queued ──claim (SKIP LOCKED, attempts+1, new lease token)──► running
               ▲                                                          │  heartbeat
               │                                                          │  (extends the lease
               ├──── fail: attempts left → run_at = now + backoff ◄───────┤   while the token
               ├──── reap: lease expired, attempts left ◄─────────────────┤   still matches)
               │                                                          │
             dead ◄── fail with no attempts left / reap of the last ◄─────┤
                                                                          ▼
                                                     done ◄── complete (same transaction as
                                                                        the handler's writes)
```

- **Claims never collide.** `FOR UPDATE SKIP LOCKED` lets concurrent workers skip rows another
  worker is taking instead of waiting, so they neither block each other nor get the same job.
- **Leases with tokens.** A claim is a short committed transaction that sets `locked_until` and
  a fresh `lease_token`. Heartbeat, complete and fail only act while the row still carries that
  token; a worker that lost its lease gets `False` and changes nothing.
- **A reaper for stale workers.** Expired leases go back to `queued`, or to `dead` if that was
  the last attempt. Attempts are counted at claim time, so a job that crashes its worker every
  time still dies eventually.
- **Exactly-once database effects, if you want them.** By default the worker runs your handler
  and `complete()` in one transaction; if the lease was lost meanwhile, the handler's writes are
  rolled back. That keeps a transaction open for the whole job, so for long jobs pass
  `transactional=False`: the handler then commits its own short transactions, completion is
  still fenced by the lease token, and your writes must be idempotent.
- **Survives connection loss.** The worker re-opens its connections with backoff (server
  restart, killed backend, network blip) and keeps going; a job whose failure could not be
  recorded is taken back by the reaper when its lease expires.
- **Transactional outbox.** `outbox.add()` writes the message in your transaction: it exists if
  and only if your change committed. A relay publishes committed messages at least once.
- **One clock.** All time comparisons use the database clock, never the workers' clocks.
  Datetimes you pass in must be timezone-aware; naive ones are rejected.
- **Hostile error text is stored safely.** Exception messages with NUL bytes or lone surrogates
  (which PostgreSQL `text` cannot hold) are escaped instead of crashing the worker or the relay.

Details and the reasons behind each choice: [`docs/DESIGN.md`](docs/DESIGN.md).

## Tests

`make check` runs ruff, `mypy --strict`, 63 tests and the demo. In the last local run the tests
took 22.7 s; all of them run against a real PostgreSQL: the bundled `pgserver` (PostgreSQL 16.2)
locally, a PostgreSQL 17 service container in CI. Each test session works in a schema of its own
that it creates and drops, so a test database URL cannot touch real taskq tables.

- **Model-based property test** (Hypothesis): random sequences of enqueue, claim, heartbeat,
  complete, fail, worker crashes, clock jumps and reaps by two workers, checked after every step
  against a model of every job's state, attempts, due time and lease owner. Pinned examples cover
  the "claim, lease expires, reaped, re-claimed, the stale worker acts" chain.
  100 random examples per run by default; `PGTASKQ_PROPERTY_EXAMPLES=1000 make test` runs more.
- **Mutation check.** Five bugs were planted one at a time; the suite fails on each:

  | planted bug | caught by |
  |---|---|
  | `claim()` may take running jobs | property test, concurrency test, 2 unit tests |
  | `complete()` ignores the lease token | property test, a unit test |
  | `heartbeat()` ignores the lease token | property test, a unit test |
  | `fail()` ignores the lease token | property test, a unit test |
  | `reap()` without `SKIP LOCKED` | the "an open reap does not block another" test only |

- **Races on separate connections:** 8 connections claim 400 jobs at once (no job claimed
  twice); 4 reapers take 300 expired jobs (each once); heartbeats race a reaper over 200 jobs
  (exactly one side wins each job); an open reap does not block another.
- **Worker:** heartbeats keep a job alive past its lease while a reaper runs; killing the
  heartbeat or the main connection mid-run is survived; error text with NUL bytes is stored
  escaped; a lost lease rolls back the handler's writes (default mode) or is fenced and counted
  (`transactional=False`); graceful stop finishes the current job.

## Limits (v0.1)

- **At-least-once execution.** A handler can run more than once (lost lease, crash after a side
  effect). Only database writes made in the handler's transaction (default mode) are
  exactly-once; e-mails, HTTP calls and other outside effects must be idempotent.
- **Polling.** Idle workers poll (1 s by default); no LISTEN/NOTIFY yet.
- **No retention.** Finished jobs and published outbox messages stay in their tables; prune
  them yourself for now. Idempotency keys stay reserved as long as their row exists.
- **No delivery order for the outbox.** A relay sends a batch in id order, but ids are assigned
  at insert, not at commit; a failed message waits for its retry while later ones go out; and
  parallel relays interleave. `message_key` is stored for consumers, not used for ordering.
- **Outbox messages retry forever** with capped backoff; there is no dead state for messages.
- **Long jobs in the default mode** keep one transaction open for the whole job (see above);
  use `transactional=False` for them.
- **One job at a time per `Worker`**, synchronous psycopg only (no asyncio). Scale out with
  more processes.
- **No benchmarks.** The only timings here are the demo and the test run above; throughput under
  load has not been measured.

## License

Apache-2.0. See [LICENSE](LICENSE).
