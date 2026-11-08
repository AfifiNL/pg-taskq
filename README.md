# pg-taskq

Durable background jobs for Python on **plain PostgreSQL**: atomic claims with
`FOR UPDATE SKIP LOCKED`, leases kept alive by heartbeats, a reaper for crashed workers, retries
with exponential backoff and jitter, a dead state for jobs that keep failing, and a
**transactional outbox** for publishing events reliably. No broker, no extension, no ORM:
about 800 lines of typed Python (psycopg 3) and SQL, docstrings included.

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
make check     # ruff, mypy --strict, and the full test suite against a real PostgreSQL
make demo      # 3 workers, retries, a crashed worker, a poison job and the outbox
```

`make demo` printed this on the machine it was written on (timings vary):

```text
crashed-worker took job 1 and stopped
worker-1: completed=13 retried=4 dead=1
worker-2: completed=13 retried=4 dead=0
worker-3: completed=14 retried=4 dead=0
jobs: {'dead': 1, 'done': 40} in 1.1 s
thumbnails written: 40
dead: image 99 after 3 attempts (ValueError: image 99: corrupt header)
outbox: published 40 messages, 0 pending
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
uv run pg-taskq reap           # requeue jobs whose lease expired
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
- **Exactly-once database effects.** The worker runs your handler and `complete()` in one
  transaction. If the lease was lost meanwhile, the handler's writes are rolled back.
- **Transactional outbox.** `outbox.add()` writes the message in your transaction: it exists if
  and only if your change committed. A relay publishes committed messages at least once.
- **One clock.** All time comparisons use the database clock, never the workers' clocks.

Details and the reasons behind each choice: [`docs/DESIGN.md`](docs/DESIGN.md).

## Tests

`make check` runs ruff, `mypy --strict` and 40 tests (about 18 s on the machine this was written
on), all against a real PostgreSQL: the bundled `pgserver` (PostgreSQL 16) locally, a
PostgreSQL 17 service container in CI.

- **Property-based** (Hypothesis): random sequences of enqueue, claim, heartbeat, complete,
  fail, worker crashes, clock jumps and reaps, checked after every step against a model of who
  owns which lease (`tests/test_properties.py` lists the invariants). 100 examples per run by
  default; `PGTASKQ_PROPERTY_EXAMPLES=1000 make test` runs more (1,000 passed in 86 s here).
  Two deliberate bugs (claiming running jobs, completing with a stale token) were planted while
  writing it, to make sure it fails on them; it does.
- **Concurrency:** 8 connections claim 400 jobs at the same time; no job is claimed twice.
- **Worker:** heartbeats keep a job alive past its lease while a reaper runs; a lost lease rolls
  back the handler's writes; graceful stop finishes the current job.

## Limits (v0.1)

- **At-least-once execution.** A handler can run more than once (lost lease, crash after a side
  effect). Only database writes made in the handler's transaction are exactly-once; e-mails,
  HTTP calls and other outside effects must be idempotent.
- **Polling.** Idle workers poll (1 s by default); no LISTEN/NOTIFY yet.
- **No retention.** Finished jobs and published outbox messages stay in their tables; prune
  them yourself for now. Idempotency keys stay reserved as long as their row exists.
- **Outbox messages retry forever** with capped backoff; there is no dead state for messages,
  and no ordering guarantee per key when several relays run in parallel.
- **One job at a time per `Worker`**, synchronous psycopg only (no asyncio). Scale out with
  more processes.
- **No benchmarks.** The only timings here are the demo and the test run above; throughput under
  load has not been measured.

## License

Apache-2.0. See [LICENSE](LICENSE).
