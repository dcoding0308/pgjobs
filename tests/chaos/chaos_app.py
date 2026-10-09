"""Worker application used by the chaos test, run in separate processes.

The handler records every execution, then performs its "side effect" in an
idempotent way (``ON CONFLICT DO NOTHING`` keyed by job id). Executions can be
duplicated when a worker is killed mid-job; effects must never be.
"""

from __future__ import annotations

import asyncio
import os
import random

from pgjobs import BackoffPolicy, Job, JobApp

app = JobApp(os.environ["PGJOBS_TEST_DATABASE_URL"], pool_max_size=12)

INJECTED_FAILURE_RATE = 0.1


@app.task("chaos.record_effect", max_attempts=50, backoff=BackoffPolicy(0.01, 0.1))
async def record_effect(job: Job) -> None:
    async with app.pool.connection() as conn:
        await conn.execute(
            "INSERT INTO chaos_executions (job_id, worker_pid) VALUES (%s, %s)",
            (job.id, os.getpid()),
        )
    await asyncio.sleep(random.uniform(0.01, 0.2))
    if random.random() < INJECTED_FAILURE_RATE:
        raise RuntimeError("injected failure")
    async with app.pool.connection() as conn:
        await conn.execute(
            "INSERT INTO chaos_effects (job_id) VALUES (%s) ON CONFLICT (job_id) DO NOTHING",
            (job.id,),
        )
