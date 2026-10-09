# pgjobs

[![CI](https://github.com/dcoding0308/pgjobs/actions/workflows/ci.yml/badge.svg)](https://github.com/dcoding0308/pgjobs/actions/workflows/ci.yml)

**A small background job queue built on PostgreSQL, with written-down delivery
guarantees and a crash test that proves them.**

Many applications run Redis and a broker only to send emails or process
uploads in the background, even though they already have PostgreSQL. pgjobs
keeps jobs in PostgreSQL, so a job can be enqueued **in the same transaction**
as the data it belongs to, and it shows what correct at-least-once delivery
takes: row locking, leases, heartbeats, fencing tokens, retries, and
graceful shutdown.

> This is a learning-oriented implementation focused on correctness and
> clarity. For production use, also consider mature libraries such as
> [Procrastinate](https://github.com/procrastinate-org/procrastinate) and
> [PgQueuer](https://github.com/janbjorge/pgqueuer).

## Results at a glance

| What | Result |
|---|---|
| Crash test | 2,000 jobs, ~20 workers killed with `SIGKILL` mid-run: **0 jobs lost**, and the idempotent test handler recorded exactly one result per job |
| Throughput (no-op jobs) | ~430–490 jobs/s with 1 slot → **~7,300–8,200 jobs/s** with 4 processes × 50 concurrency |
| Pickup latency | **p50 7.4–7.8 ms** with LISTEN/NOTIFY vs. ~660 ms with 1 s polling |
| Tests | 96 unit and integration tests + chaos test, ruff, strict mypy, CI on Python 3.11 and 3.12 |

![pgjobs dashboard](docs/images/dashboard.png)

## Features

- **Transactional enqueue**: the job exists if and only if your transaction commits
- **`FOR UPDATE SKIP LOCKED`** claiming: workers never block each other or double-claim
- **Leases + heartbeats** for long jobs; **reaper** recovers jobs from crashed workers
- **Fencing tokens**: a paused worker that lost its lease cannot overwrite the new owner's result
- **Retries** with exponential backoff and full jitter; **dead-lettering** after `max_attempts`
- `PermanentJobError` to fail fast; per-task **timeouts**
- **Priorities**, **delayed jobs**, **deduplication keys**, multiple **queues**
- **LISTEN/NOTIFY** wake-ups with a polling fallback (`--no-listen` for PgBouncer)
- **Graceful shutdown** on SIGTERM that refunds interrupted jobs' attempts
- Optional **web dashboard**: live queue numbers, recent jobs, full error tracebacks, retry dead jobs
- Async and sync handlers; two runtime dependencies (`psycopg`, `psycopg-pool`)

## Quick start

Requirements: Python 3.11+, [uv](https://docs.astral.sh/uv/), Docker.

```bash
git clone https://github.com/dcoding0308/pgjobs.git && cd pgjobs
docker compose up -d --wait          # PostgreSQL 16 on localhost:5433
uv sync
export DATABASE_URL=postgresql://pgjobs:pgjobs@localhost:5433/pgjobs
uv run pgjobs migrate
```

Define tasks:

```python
# myapp/jobs.py
import os
from pgjobs import Job, JobApp, PermanentJobError

app = JobApp(os.environ["DATABASE_URL"])


@app.task("emails.welcome", max_attempts=5, timeout_seconds=30)
async def send_welcome_email(job: Job) -> None:
    user = await load_user(job.payload["user_id"])
    if user is None:
        raise PermanentJobError("user no longer exists")  # no retries
    await email_client.send(user.email, idempotency_key=str(job.id))
```

Enqueue, optionally inside your own transaction:

```python
async with app:
    async with app.pool.connection() as conn, conn.transaction():
        user_id = await create_user(conn, email)
        await send_welcome_email.enqueue(
            {"user_id": user_id}, dedupe_key=f"welcome:{user_id}", connection=conn
        )
```

Run workers (as many processes as you like):

```bash
uv run pgjobs worker myapp.jobs:app --concurrency 20
uv run pgjobs stats
uv run pgjobs requeue-dead
```

A complete runnable example is in [`examples/signup.py`](examples/signup.py).

## Dashboard

```bash
uv run pgjobs dashboard                 # http://127.0.0.1:8080
uv run pgjobs dashboard --read-only     # hide the "retry dead jobs" action
```

Install with `pip install "pgjobs[dashboard]"` (adds Starlette and Uvicorn).
To see it with a realistic mixed workload (fast, flaky, slow, and failing
jobs across four queues), see [`examples/demo.py`](examples/demo.py).

- Refreshes every 2 seconds; the URL mirrors the view, so
  `#status=dead&job=42` links straight to a failed job.
- Its queries are designed around partial indexes. A test runs `EXPLAIN` on
  the exact SQL the dashboard uses, with 50,000 finished jobs in the table,
  and fails if any plan contains a sequential scan. (It checks query plans,
  not response times.)
- **No authentication.** It binds to localhost by default; to expose it,
  put it behind your own authentication. The retry action requires a
  custom header, which protects against cross-site request forgery but is
  *not* authentication. All job data is rendered as text (no XSS through
  payloads or error messages), and responses carry a strict
  Content-Security-Policy.

## How it works

```
 producer ── INSERT + NOTIFY (on commit) ──► pgjobs_jobs ◄── claim (SKIP LOCKED) ── worker
                                                 ▲                                   │
                                                 └── heartbeat / succeeded / retry ──┘
                                                     (every write checks lease_token)
```

1. **Claim**: a worker atomically picks up to N due jobs with
   `FOR UPDATE SKIP LOCKED`, sets them to `running`, increments `attempts`,
   and gives each a new random `lease_token` and a `locked_until` deadline.
2. **Run**: each job runs in its own asyncio task. A heartbeat extends the
   lease every `lease/3` seconds.
3. **Record**: success, retry, or dead is written with
   `WHERE id = ? AND lease_token = ?`, so only the current owner can do it.
4. **Recover**: if a worker dies, its leases expire and any worker's reaper
   returns those jobs to the queue.

Read more:
- [docs/delivery-semantics.md](docs/delivery-semantics.md): the exact
  guarantees, failure by failure
- [docs/architecture.md](docs/architecture.md): components and design
  decisions

## Delivery guarantee

**At-least-once.** A job can run more than once if a worker crashes after
doing the work but before recording success. That is unavoidable for any
queue, so handlers should be idempotent (see the techniques in
[delivery-semantics.md](docs/delivery-semantics.md)). An idempotent handler
turns repeated executions into a single effect. The crash test demonstrates
this for one specific case, a database write keyed by job id with
`ON CONFLICT DO NOTHING`. It does not prove exactly-once effects for
arbitrary side effects: an email or a payment call is only safe if that
downstream system supports idempotency (for example an idempotency key).

## Testing

```bash
docker compose up -d --wait
make db    # also creates a separate pgjobs_test database
export PGJOBS_TEST_DATABASE_URL=postgresql://pgjobs:pgjobs@localhost:5433/pgjobs_test
uv run pytest -m "not chaos"     # unit + integration, ~6 s
uv run pytest -m chaos -s        # crash test, ~10 s
```

Without `PGJOBS_TEST_DATABASE_URL`, only the unit tests run.

**The chaos test** ([tests/chaos/test_chaos.py](tests/chaos/test_chaos.py))
starts 4 worker processes on 2,000 jobs, injects a 10% handler failure rate,
and `SIGKILL`s a random worker every 0.2–0.8 s for 10 s, restarting it each
time. It then checks:

1. every job reached `succeeded` and none is `dead`;
2. killed workers really were in the middle of jobs (the crash path ran);
3. there is exactly one effect row per job.

A typical run: `2000 jobs, 20 SIGKILLs, 2348 executions, 298 jobs ran more
than once, 2000 effects, 10.6s`. The ~300 repeated executions show
at-least-once delivery happening; the 2,000 effect rows show that the
test's idempotent database write absorbed every duplicate.

## Benchmarks

```bash
export PGJOBS_BENCH_DATABASE_URL=postgresql://pgjobs:pgjobs@localhost:5433/pgjobs_test
uv run python benchmarks/run.py         # truncates pgjobs_jobs; saves a run file
uv run python benchmarks/summarize.py   # ranges across all saved runs
```

Measured on an Apple Silicon MacBook Air, Python 3.12, PostgreSQL 16 in
Docker on the same machine, 10,000 no-op jobs, three runs. Each run's raw
results are saved in [`benchmarks/results/`](benchmarks/results/):

| Worker processes × concurrency | Jobs / second |
|---|---|
| 1 × 1 | 429 – 489 |
| 1 × 10 | 2,657 – 2,991 |
| 1 × 50 | 5,499 – 5,695 |
| 2 × 50 | 6,590 – 7,111 |
| 4 × 50 | 7,268 – 8,188 |

| Pickup latency (idle worker, 40 samples) | p50 | p95 | max |
|---|---|---|---|
| LISTEN/NOTIFY (+1 s poll fallback) | 7.4 – 7.8 ms | 10.0 – 10.1 ms | 11.5 – 16.1 ms |
| 1 s polling only | 660 – 661 ms | 959 – 961 ms | 960 – 964 ms |

**How to read these numbers:**
- With a single slot, every job costs two sequential transactions (claim,
  then complete), so throughput is bounded by round-trip latency.
  Concurrency hides that latency.
- Gains flatten beyond 2 processes. On one laptop the workers and the
  database compete for the same CPU cores, and every job is two row
  updates, which the database must write and later vacuum.
- These are no-op jobs. Real jobs are dominated by their own work, so queue
  overhead matters less.
- Polling latency averages about half the poll interval, as expected;
  NOTIFY removes that wait.

## Limitations

- At-least-once only; handlers must be idempotent.
- Synchronous handlers run in threads, which Python cannot stop. After a
  timeout, a shutdown, or a lost lease, the thread keeps running (and keeps
  causing side effects) while the job may be retried elsewhere. Use async
  handlers when cancellation matters.
- `pgjobs migrate` bootstraps the v1 schema with `CREATE ... IF NOT EXISTS`.
  It does not alter existing tables, so it is not a versioned migration
  system; future schema changes would need one.
- Finished jobs are not pruned automatically.
- No recurring (cron) schedules or result storage.
- The dashboard has no authentication of its own; keep it on localhost or
  behind your own auth.
- Not designed for tens of thousands of jobs per second sustained; a
  dedicated broker fits that better.

## Project layout

```
src/pgjobs/
  app.py        JobApp: pool, task registration, enqueue
  worker.py     main loop, heartbeats, retries, shutdown
  queries.py    all SQL (claiming, fencing, reaping)
  schema.py     table and indexes
  tasks.py      task definitions and registry
  backoff.py    exponential backoff with jitter
  cli.py        command-line interface
  dashboard/    optional web dashboard (Starlette + plain HTML/CSS/JS)
tests/
  unit/         no database needed
  integration/  SQL layer and worker against PostgreSQL
  chaos/        SIGKILL crash test
benchmarks/     throughput and latency
examples/       runnable signup + welcome-email example
docs/           delivery semantics, architecture, roadmap
```
