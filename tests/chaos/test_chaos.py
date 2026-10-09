"""Kill worker processes with SIGKILL while they process jobs.

Invariants checked:

1. No job is lost: every job eventually reaches ``succeeded``.
2. The crash path was really exercised: killed workers had started jobs
   that other workers then had to redo.
3. Idempotent handlers give exactly-once effects: one effect row per job.
"""

from __future__ import annotations

import asyncio
import os
import random
import signal
import sys
import time
from pathlib import Path

import pytest

from pgjobs import JobApp

pytestmark = pytest.mark.chaos

JOB_COUNT = 2000
WORKER_PROCESSES = 4
KILL_PHASE_SECONDS = 10.0
DEADLINE_SECONDS = 120.0
MAX_ATTEMPTS = 50  # jobs face 10% injected failures plus repeated SIGKILLs
REPO_ROOT = Path(__file__).resolve().parents[2]


async def start_worker(log_dir: Path, index: int) -> asyncio.subprocess.Process:
    log_file = (log_dir / f"worker-{index}.log").open("w")
    return await asyncio.create_subprocess_exec(
        sys.executable,
        "-m",
        "pgjobs",
        "worker",
        "tests.chaos.chaos_app:app",
        "--concurrency=8",
        "--lease-seconds=1",
        "--reap-interval=0.3",
        "--poll-interval=0.5",
        "--shutdown-grace=5",
        cwd=REPO_ROOT,
        stdout=log_file,
        stderr=log_file,
        env={**os.environ, "PYTHONPATH": str(REPO_ROOT)},
    )


async def count_jobs(app: JobApp) -> tuple[int, int]:
    """Return (unfinished, dead) job counts."""
    async with app.pool.connection() as conn:
        cur = await conn.execute(
            """
            SELECT count(*) FILTER (WHERE status IN ('queued', 'running')),
                   count(*) FILTER (WHERE status = 'dead')
            FROM pgjobs_jobs
            """
        )
        row = await cur.fetchone()
    assert row is not None
    return int(row[0]), int(row[1])


async def test_no_job_is_lost_when_workers_are_killed(app: JobApp, tmp_path: Path) -> None:
    async with app.pool.connection() as conn:
        await conn.execute("DROP TABLE IF EXISTS chaos_executions, chaos_effects")
        await conn.execute(
            "CREATE TABLE chaos_executions (job_id BIGINT NOT NULL, worker_pid INT NOT NULL)"
        )
        await conn.execute("CREATE TABLE chaos_effects (job_id BIGINT PRIMARY KEY)")
    for _ in range(JOB_COUNT):
        # The producer decides max_attempts. This process never imports the
        # task definition, so it must pass the value explicitly.
        await app.enqueue("chaos.record_effect", max_attempts=MAX_ATTEMPTS)

    rng = random.Random(2024)
    workers = [await start_worker(tmp_path, i) for i in range(WORKER_PROCESSES)]
    killed_pids: list[int] = []
    started = time.monotonic()
    try:
        while True:
            unfinished, dead = await count_jobs(app)
            assert dead == 0, f"{dead} job(s) died; logs in {tmp_path}"
            if unfinished == 0:
                break
            elapsed = time.monotonic() - started
            assert elapsed < DEADLINE_SECONDS, f"jobs still pending; logs in {tmp_path}"
            await asyncio.sleep(rng.uniform(0.2, 0.8))
            if elapsed < KILL_PHASE_SECONDS:
                victim = rng.randrange(len(workers))
                workers[victim].send_signal(signal.SIGKILL)
                await workers[victim].wait()
                killed_pids.append(workers[victim].pid)
                workers[victim] = await start_worker(tmp_path, len(workers) + len(killed_pids))
    finally:
        for process in workers:
            if process.returncode is None:
                process.send_signal(signal.SIGTERM)
        await asyncio.gather(*(process.wait() for process in workers))

    async with app.pool.connection() as conn:
        cur = await conn.execute(
            """
            SELECT
                (SELECT count(*) FROM chaos_effects),
                (SELECT count(*) FROM chaos_executions),
                (SELECT count(*) FROM (
                    SELECT job_id FROM chaos_executions GROUP BY job_id HAVING count(*) > 1
                ) AS repeated),
                (SELECT count(*) FROM chaos_executions WHERE worker_pid = ANY(%s))
            """,
            (killed_pids,),
        )
        row = await cur.fetchone()
    assert row is not None
    effects, executions, repeated_jobs, executions_in_killed_workers = row

    print(
        f"\nchaos: {JOB_COUNT} jobs, {len(killed_pids)} SIGKILLs, {executions} executions "
        f"({executions_in_killed_workers} in killed workers), "
        f"{repeated_jobs} jobs ran more than once, {effects} effects, "
        f"{time.monotonic() - started:.1f}s"
    )
    assert killed_pids
    assert executions_in_killed_workers > 0
    assert effects == JOB_COUNT
