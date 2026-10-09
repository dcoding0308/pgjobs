# Delivery semantics

This document states exactly what pgjobs guarantees, and what it does not.

## The guarantee: at-least-once

Once `enqueue` commits, the job will be executed **at least once**, as long
as the database survives and some worker eventually runs. A job may be
executed **more than once**. Exactly-once *execution* is impossible when a
worker can crash between doing the work and recording that it did it; no
queue can tell "crashed before the side effect" apart from "crashed after it".

What you can get is **exactly-once effects**, by making handlers idempotent.
This is the handler's responsibility, and it is only as strong as the
system the effect lands in: a database write can be made idempotent with a
unique key, but an email or a payment call is only safe if that downstream
service accepts an idempotency key. The chaos test verifies the first kind
(one row per job, `ON CONFLICT DO NOTHING`), not arbitrary side effects.

| Technique | Example |
|---|---|
| Natural idempotency | `UPDATE users SET plan = 'pro' WHERE id = 42` |
| Check-then-act, recording completion | the `welcomed_at` column in `examples/signup.py` |
| Unique constraint on the effect | `INSERT ... ON CONFLICT (job_id) DO NOTHING` (used by the chaos test) |
| Idempotency key sent downstream | pass `job.id` as the `Idempotency-Key` header to a payment API |

## Job states

```
            enqueue
               │
               ▼
   ┌──────► queued ──── claim ────► running ──── success ────► succeeded
   │           ▲                      │
   │           │  retry (backoff)     ├── exception, attempts left
   │           └──────────────────────┤
   │                                  ├── exception on last attempt,
   │                                  │   PermanentJobError,
   │                                  │   or unknown task ───────► dead
   │                                  │
   │  lease expired (reaper)          │
   └──────────────────────────────────┘   (dead if it was the last attempt)

   requeue-dead: dead ──► queued with attempts reset to 0
```

## Rules, failure by failure

**Two workers claim at the same moment.** The claim query uses
`SELECT ... FOR UPDATE SKIP LOCKED`. The first transaction locks the row; the
second skips it rather than waiting. A row is only ever claimed by one
worker at a time. (Tested: `test_concurrent_claims_never_return_the_same_job`.)

**A worker crashes (SIGKILL, OOM, power loss) mid-job.** Its lease
(`locked_until`) stops being extended. Once it expires, any worker's reaper
moves the job back to `queued`. The crash counts as an attempt, so a job that
crashes its worker every time (a *poison* job) eventually goes to `dead`
instead of taking down workers forever.
(Tested: chaos test, `test_reaper_buries_jobs_that_crash_on_their_last_attempt`.)

**What a lease means, precisely.** The `lease_token` decides ownership; the
`locked_until` deadline only decides when the reaper *may* take a job back.
A heartbeat that arrives after the deadline but before any reaper has acted
still renews the lease. That is deliberate: no other worker can own the job
at that point, so abandoning it would only cause a needless re-run. Once a
reaper has requeued the job, the old token no longer matches anything.
(Tested: `test_lease_deadline_triggers_the_reaper_but_the_token_decides_ownership`.)
A stricter rule (renew only before the deadline) would add duplicate
executions without preventing any conflicting write.

**A worker pauses, then wakes up after its lease was taken over** (a long GC
pause, a frozen VM, a network partition). Every claim issues a new random
`lease_token`, and every later write must match it:
`... WHERE id = $1 AND lease_token = $2`. The stale worker's writes match zero
rows, so it cannot mark the job succeeded or failed over the new owner. Its
heartbeat notices the lost lease and cancels the handler. This is a
*fencing token*. (Tested: `test_stale_lease_token_cannot_change_the_job`.)

**A worker is stopped gracefully (SIGTERM).** It stops claiming, waits up to
`shutdown_grace_seconds` for running jobs, then cancels the rest and releases
them back to the queue. Released jobs get their attempt refunded, because the
job did not fail. (Tested: `test_graceful_shutdown_*`.)

**The handler raises.** The job is retried after
`uniform(0, min(max, base * 2^(attempt-1)))` seconds ("full jitter"), until
`max_attempts` is reached; then it is `dead`, with the traceback in
`last_error`. Raise `PermanentJobError` to skip retries for errors that will
never succeed.

**The handler hangs.** With `timeout_seconds` set, an async handler is
cancelled and the attempt counts as a failure.

**Synchronous handlers cannot be cancelled.** They run in a thread via
`asyncio.to_thread`, and Python cannot stop a thread. After a timeout, a
graceful shutdown, or a lost lease, the asyncio side gives up on the job,
but the thread keeps running and can keep producing side effects while the
job is retried elsewhere. Use async handlers when cancellation matters, and
make side effects idempotent either way.

**The database is unreachable.** Workers log the error and keep retrying.
Heartbeat failures are tolerated until the lease actually expires.

**The application commits a row and the job in one transaction.** Pass
`connection=` to `enqueue`. The job exists if and only if the transaction
commits; `NOTIFY` is also delivered only on commit. This is the main
advantage of a PostgreSQL queue over a separate broker, where "row saved but
message lost" (or the reverse) is a classic bug.

**The same logical job is enqueued twice.** Pass a `dedupe_key`. A unique
index on `(queue, dedupe_key)` makes the second enqueue return the first
job's id with `created=False`. Dedupe keys are permanent: re-enqueueing the
same key after the job finished is also ignored.

## When workers are woken up

Workers sleep until one of these happens, and otherwise poll every
`poll_interval_seconds` as a safety net:

| Event | Wake-up |
|---|---|
| A job is enqueued (immediate or delayed) | `NOTIFY` on its queue, sent on commit |
| A job is scheduled for retry | `NOTIFY` on its queue |
| The reaper recovers jobs from a dead worker | `NOTIFY` to all workers |
| `requeue-dead` | `NOTIFY` |
| A job in this worker finishes | local wake-up (a slot is free) |
| The next queued job becomes due | each idle worker asks the database for the earliest `run_after` and sleeps exactly until then |

A notification only makes a worker re-check the table; the claim query
decides what it actually runs. A missed notification therefore costs
latency, never correctness.

## Clocks

All times (`run_after`, `locked_until`) are computed by PostgreSQL's `now()`,
never by workers. Workers on machines with skewed clocks therefore agree on
when a lease has expired.

## Settings that are decided at enqueue time

`queue` and `max_attempts` are stored on the row when the job is enqueued. If
the producer has the task registered, its defaults are used; otherwise
the producer must pass them, or the global defaults (`default`, 5) apply.
Changing a task's `max_attempts` in the worker code does not affect jobs
already in the table.

## Known limitations

- Delivery order is by priority, then `run_after`, then id, but with many
  concurrent workers, completion order is not guaranteed.
- `LISTEN` does not work through poolers in transaction mode (e.g.
  PgBouncer); run workers with `--no-listen` there and rely on polling.
- Finished jobs are kept forever; prune them yourself, e.g.
  `DELETE FROM pgjobs_jobs WHERE status = 'succeeded' AND finished_at < now() - interval '7 days'`.
- Very high throughput (tens of thousands of jobs per second) creates many
  dead tuples and puts pressure on autovacuum; a dedicated broker is a better
  fit at that scale.
