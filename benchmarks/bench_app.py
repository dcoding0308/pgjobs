"""Worker application used by the benchmarks: a single no-op task.

A no-op handler measures the queue's own overhead (claiming, recording
results, round trips to PostgreSQL), not the cost of any real work.
"""

from __future__ import annotations

import os

from pgjobs import Job, JobApp

# Connections are held only for short claim/complete queries, not for the
# whole job, so a small pool serves high concurrency. Total connections are
# processes x pool_max_size and must stay under PostgreSQL's max_connections.
app = JobApp(os.environ["PGJOBS_BENCH_DATABASE_URL"], pool_max_size=10)


@app.task("bench.noop")
async def noop(job: Job) -> None:
    return None
