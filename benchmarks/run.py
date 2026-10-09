"""Measure throughput and pickup latency.

    export PGJOBS_BENCH_DATABASE_URL=postgresql://pgjobs:pgjobs@localhost:5433/pgjobs
    uv run python benchmarks/run.py

WARNING: this truncates the pgjobs_jobs table. Point it at a scratch database.

Throughput: N no-op jobs are inserted at once while worker *processes* are
already idle and waiting; we time until every job has succeeded.

Pickup latency: one job at a time is enqueued into an idle worker and we time
from the enqueue commit until the handler starts, with and without
LISTEN/NOTIFY.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import platform
import statistics
import sys
import time
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path

from pgjobs import Job, JobApp, Worker

REPO_ROOT = Path(__file__).resolve().parents[1]
RESULTS_DIR = REPO_ROOT / "benchmarks" / "results"


@dataclass
class ThroughputResult:
    processes: int
    concurrency_per_process: int
    jobs: int
    seconds: float
    jobs_per_second: float


@dataclass
class LatencyResult:
    mode: str
    samples: int
    p50_ms: float
    p95_ms: float
    max_ms: float


async def reset(app: JobApp) -> None:
    async with app.pool.connection() as conn:
        await conn.execute("TRUNCATE pgjobs_jobs RESTART IDENTITY")


async def bulk_insert(app: JobApp, count: int) -> None:
    async with app.pool.connection() as conn:
        await conn.execute(
            """
            INSERT INTO pgjobs_jobs (task_name, queue)
            SELECT 'bench.noop', 'default' FROM generate_series(1, %s)
            """,
            (count,),
        )
        await conn.execute("SELECT pg_notify('pgjobs', 'default')")


async def unfinished(app: JobApp) -> int:
    async with app.pool.connection() as conn:
        cur = await conn.execute("SELECT count(*) FROM pgjobs_jobs WHERE status <> 'succeeded'")
        row = await cur.fetchone()
    assert row is not None
    return int(row[0])


async def measure_throughput(
    app: JobApp, processes: int, concurrency: int, jobs: int
) -> ThroughputResult:
    await reset(app)
    workers = [
        await asyncio.create_subprocess_exec(
            sys.executable,
            "-m",
            "pgjobs",
            "--log-level=WARNING",
            "worker",
            "benchmarks.bench_app:app",
            f"--concurrency={concurrency}",
            "--poll-interval=0.5",
            cwd=REPO_ROOT,
            env={**os.environ, "PYTHONPATH": str(REPO_ROOT)},
        )
        for _ in range(processes)
    ]
    try:
        await asyncio.sleep(2.0)  # let workers connect and go idle
        started = time.perf_counter()
        await bulk_insert(app, jobs)
        while await unfinished(app) > 0:  # noqa: ASYNC110 - polling the database
            await asyncio.sleep(0.05)
        elapsed = time.perf_counter() - started
    finally:
        for process in workers:
            process.terminate()
        await asyncio.gather(*(process.wait() for process in workers))
    return ThroughputResult(processes, concurrency, jobs, elapsed, jobs / elapsed)


async def measure_latency(app: JobApp, use_listen_notify: bool, samples: int) -> LatencyResult:
    await reset(app)
    started_at: asyncio.Future[float] | None = None

    async def record_start(job: Job) -> None:
        if started_at is not None and not started_at.done():
            started_at.set_result(time.perf_counter())

    task_name = f"bench.latency.{'notify' if use_listen_notify else 'poll'}"
    if task_name not in app.registry:
        app.task(task_name)(record_start)

    worker = Worker(
        app,
        concurrency=1,
        poll_interval_seconds=1.0,
        reap_interval_seconds=60,
        use_listen_notify=use_listen_notify,
    )
    run = asyncio.create_task(worker.run())
    loop = asyncio.get_running_loop()
    latencies: list[float] = []
    try:
        await asyncio.sleep(1.0)
        for _ in range(samples):
            started_at = loop.create_future()
            enqueued = time.perf_counter()
            await app.enqueue(task_name)
            latencies.append((await asyncio.wait_for(started_at, 10) - enqueued) * 1000)
            # Random-ish idle gap so polling is sampled at different phases.
            await asyncio.sleep(0.05 + (len(latencies) % 7) * 0.1)
    finally:
        worker.stop()
        await run

    latencies.sort()
    return LatencyResult(
        mode="LISTEN/NOTIFY + 1s poll" if use_listen_notify else "1s polling only",
        samples=samples,
        p50_ms=statistics.median(latencies),
        p95_ms=latencies[int(0.95 * (len(latencies) - 1))],
        max_ms=latencies[-1],
    )


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--jobs", type=int, default=10_000)
    parser.add_argument("--latency-samples", type=int, default=40)
    args = parser.parse_args()

    database_url = os.environ.get("PGJOBS_BENCH_DATABASE_URL")
    if not database_url:
        sys.exit("set PGJOBS_BENCH_DATABASE_URL (the table will be truncated)")

    app = JobApp(database_url, pool_max_size=5)
    async with app:
        await app.migrate()

        throughput: list[ThroughputResult] = []
        for processes, concurrency in [(1, 1), (1, 10), (1, 50), (2, 50), (4, 50)]:
            result = await measure_throughput(app, processes, concurrency, args.jobs)
            throughput.append(result)
            print(
                f"throughput: {processes} proc x {concurrency:>2} -> "
                f"{result.jobs_per_second:,.0f} jobs/s ({result.seconds:.1f}s)",
                flush=True,
            )

        latency = [
            await measure_latency(app, use_listen_notify=True, samples=args.latency_samples),
            await measure_latency(app, use_listen_notify=False, samples=args.latency_samples),
        ]
        for result in latency:
            print(
                f"latency: {result.mode}: p50={result.p50_ms:.1f}ms "
                f"p95={result.p95_ms:.1f}ms max={result.max_ms:.1f}ms",
                flush=True,
            )
        await reset(app)

    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    recorded_at = datetime.now(UTC)
    report = {
        "recorded_at": recorded_at.isoformat(timespec="seconds"),
        "machine": f"{platform.system()} {platform.machine()}, Python {platform.python_version()}",
        "throughput": [asdict(result) for result in throughput],
        "latency": [asdict(result) for result in latency],
    }
    # Every run is kept so published ranges can be checked against raw data.
    path = RESULTS_DIR / f"run-{recorded_at:%Y%m%dT%H%M%SZ}.json"
    path.write_text(json.dumps(report, indent=2) + "\n")
    print(f"saved {path.relative_to(REPO_ROOT)}")


if __name__ == "__main__":
    asyncio.run(main())
