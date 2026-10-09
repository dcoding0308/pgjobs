# Architecture

## Components

```
 Producer (web app, script, CLI)                 Worker process (one or more)
 ─────────────────────────────────               ───────────────────────────────────────
  app.enqueue(...)                                main loop
   │  INSERT job (+ NOTIFY on commit)              │  1. reap expired leases (every N s)
   │  optionally inside the caller's               │  2. claim up to `free slots` jobs
   │  own transaction                              │  3. start one asyncio task per job
   ▼                                               │  4. sleep until NOTIFY, a job
 ┌──────────────────────────────────┐              │     finishes, or poll interval
 │ PostgreSQL                       │◄─────────────┤
 │                                  │              │ per job:
 │  pgjobs_jobs                     │◄─────────────┤  handler task ─ runs user code
 │   partial index: queued rows     │              │  heartbeat task ─ extends lease
 │   partial index: running leases  │              │  then: succeeded / retry / dead
 │   unique index: dedupe keys      │              │  (each write fenced by lease_token)
 │                                  │              │
 │  channel "pgjobs" (LISTEN/NOTIFY)│─────────────►│ listener connection (autocommit)
 └──────────────────────────────────┘              ▼
```

## Source layout

| File | Responsibility |
|---|---|
| `schema.py` | Table, indexes, constraints; applied under an advisory lock |
| `queries.py` | Every SQL statement. Owns the correctness rules (SKIP LOCKED, fencing, DB clock) |
| `app.py` | `JobApp`: connection pool, task registration, producer API |
| `tasks.py` | `Task` and `TaskRegistry`; runs sync handlers in threads |
| `worker.py` | Main loop, heartbeats, outcome recording, graceful shutdown |
| `backoff.py` | Exponential backoff with full jitter |
| `cli.py` | `migrate`, `worker`, `enqueue`, `stats`, `requeue-dead`, `dashboard` |
| `dashboard/` | Optional Starlette app: JSON API plus one static HTML/CSS/JS page |

`queries.py` contains no business logic beyond SQL, and `worker.py` contains
no SQL. That split lets the SQL layer be tested on its own
(`tests/integration/test_queries.py`) and keeps the worker readable.

## Design decisions

**PostgreSQL instead of Redis or RabbitMQ.** Most applications already run
PostgreSQL. Keeping jobs there removes a moving part and, more importantly,
allows enqueueing in the same transaction as the business write. The cost is
lower peak throughput and extra vacuum load, which matters only at volumes
far beyond what most applications need.

**One table with a status column**, instead of separate queued/finished
tables. Simpler to reason about and query. Partial indexes keep the hot
paths fast regardless of how many finished rows accumulate: the claim index
only contains `queued` rows.

**Leases with heartbeats** instead of holding a row lock for the whole job.
Holding a transaction open while a handler runs for minutes would pin a
connection per job and block vacuum. Short transactions plus a lease mean a
connection is used only for milliseconds per state change.

**Fencing tokens** instead of trusting that a worker still owns its job. A
lease alone is not enough: a paused worker cannot know its lease expired.

**asyncio with a concurrency limit per process.** Typical background jobs
wait on I/O (HTTP calls, emails, database queries), so one process can run
many jobs concurrently. CPU-heavy work should scale with more processes.

**LISTEN/NOTIFY plus "sleep until next due".** Every state change that
creates runnable work sends a NOTIFY, and an idle worker sleeps exactly until
the earliest queued job is due. Polling remains a safety net for missed
notifications (e.g. while the listener reconnects). See "When workers are
woken up" in delivery-semantics.md.

**Schema bootstrap, not migrations.** `pgjobs migrate` applies idempotent
`CREATE ... IF NOT EXISTS` statements. That is enough for v1, but it cannot
alter an existing table; a schema change in a later version would need
versioned migrations.

**Producer-side defaults.** `queue` and `max_attempts` are written on the row
at enqueue time, so the worker never needs to guess a policy for a job.

## Connection budget

Each process opens `pool_max_size` pooled connections plus one listener
connection. Total connections are roughly
`processes × (pool_max_size + 1)` and must stay below PostgreSQL's
`max_connections` (100 by default). Because connections are held only for
short queries, a pool of about 10 serves a concurrency of 50 comfortably.
This limit was hit during benchmarking: four processes with 60-connection
pools exhausted the server.

## Dashboard

The dashboard is an optional extra so the core library keeps two runtime
dependencies. It is a JSON API (`/api/summary`, `/api/jobs`,
`/api/jobs/{id}`, `POST /api/requeue-dead`) plus one static page that polls
it every two seconds. There is no frontend build step.

Its queries must stay cheap however large the table grows, because it
refreshes constantly. Counting all rows by status would scan every finished
job, so instead each number comes from a partial index: queued, running,
dead, and recently finished jobs (`finished_at`). A test checks the query
plans with `EXPLAIN` against 50,000 finished rows.

Security: it binds to localhost by default and has no authentication of its
own. The only write action requires a custom `X-Pgjobs-Action` header, which
a cross-site form cannot send (CSRF protection). Job data is inserted into
the page with `textContent` only (no XSS), and responses carry a strict
Content-Security-Policy that blocks inline scripts.
