# Roadmap and progress

## Goal

A small, understandable background job queue on PostgreSQL whose delivery
guarantees are written down and proven by automated crash tests. It is a
learning-oriented implementation, not a replacement for mature libraries such
as Procrastinate or PgQueuer.

## Milestones

### M1: Core library ✅
- [x] Schema with partial indexes and a running-implies-lease constraint
- [x] Enqueue with priority, delay, max attempts, dedupe keys
- [x] Transactional enqueue (join the caller's transaction)
- [x] Claim with `FOR UPDATE SKIP LOCKED`
- [x] Lease tokens as fencing tokens for every state change
- [x] Heartbeats that extend leases while a handler runs
- [x] Reaper for expired leases (crashed workers)
- [x] Retries with exponential backoff and full jitter
- [x] Dead-lettering after max attempts; `PermanentJobError` to skip retries
- [x] Per-task timeouts
- [x] LISTEN/NOTIFY wake-ups with polling fallback
- [x] Graceful shutdown that refunds the attempt of interrupted jobs
- [x] CLI: `migrate`, `worker`, `enqueue`, `stats`, `requeue-dead`

### M2: Verification ✅
- [x] Unit tests (backoff, task registry, CLI)
- [x] Integration tests for the SQL layer and the worker
- [x] Chaos test that SIGKILLs worker processes
- [x] Run integration and chaos tests locally against PostgreSQL 16 (chaos: 8/8 runs)
- [x] Lint (ruff) and strict type checking (mypy)

### M3: Evidence ✅
- [x] Throughput benchmark vs. number of workers and concurrency
- [x] Pickup latency benchmark: LISTEN/NOTIFY vs. polling only
- [x] Record measured results in the README (real numbers only)

### M4: Presentation
- [x] Runnable example application (verified end to end)
- [x] README: problem, design, guarantees, results, limitations
- [x] Architecture and delivery-semantics docs
- [x] GitHub Actions CI workflow with a PostgreSQL service (runs once published)
- [x] Web dashboard: queue overview, recent jobs, job details, retry dead jobs
- [ ] Demo recording (terminal GIF or video)
- [ ] Publish the repository (with the owner's approval)

## Issues found and fixed during development

- Default task names for nested functions contain `<locals>` and are not
  stable across processes; they are now rejected with a clear error.
- A graceful shutdown on a job's final attempt pushed `attempts` past
  `max_attempts`; shutdown now refunds the attempt via a dedicated `release`.
- The chaos test intermittently timed out: the producer enqueued without the
  task definition, so jobs got the default 5 attempts instead of 50. Fixed
  in the test, documented as "settings decided at enqueue time", and the
  test now fails fast on any dead job.
- Benchmarks exhausted PostgreSQL's 100 connections (4 processes × 60-
  connection pools). Pools were resized and the connection budget is
  documented in docs/architecture.md.
- The dashboard overflowed horizontally on narrow screens: wide tables
  stretched their CSS grid column. Fixed with `min-width: 0` so tables
  scroll inside their panel.
- Worker logs showed "Traceback (most recent call last):" as the error
  summary; both the logs and the dashboard now show the last traceback line.
- Found during a manual demo: retries and delayed jobs ran up to one poll
  interval (5 s) late, because only new jobs send a NOTIFY. An idle worker
  now asks the database when the next queued job is due and sleeps exactly
  until then. Covered by a test that uses a 30 s poll interval.
- Tests interfered with a running demo worker sharing the same database.
  Tests and benchmarks now use a separate `pgjobs_test` database.

### From an independent code review (each bug reproduced by a failing test first)

- Delayed jobs sent no NOTIFY, so an idle worker could start one up to a
  poll interval late. Every new job now notifies.
- Retries and reaped jobs did not wake idle peer workers; they now notify.
- `max_attempts=0` (and `queue=""`) silently fell back to defaults because
  of `x or default`; explicit values are now validated.
- NaN and infinity passed `<= 0` checks for intervals, timeouts, and backoff
  settings; all are now validated as finite.
- The dashboard EXPLAIN test checked copies of the SQL and skipped one
  query; it now checks the exact statements the dashboard runs.
- Only the latest benchmark run was kept; every run is now saved, and the
  README ranges are computed from the saved files.
- Docs narrowed: "exactly-once effects" applies to idempotent handlers (the
  chaos test proves it for a keyed database write only); synchronous
  handlers keep running after timeout, shutdown, or lease loss; `migrate`
  is a v1 bootstrap, not a migration system.
- Declined: making heartbeats fail after the lease deadline. The token
  already decides ownership; a stricter rule would add duplicate runs
  without preventing any conflicting write. Documented and tested instead.

## Deliberately out of scope for v1

- Cron or recurring schedules
- Job result storage and return values
- Automatic pruning of finished jobs
- Multi-database or sharded deployments
