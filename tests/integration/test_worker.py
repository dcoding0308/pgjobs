"""End-to-end tests of the worker against a real database."""

from __future__ import annotations

import asyncio
import time
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

from pgjobs import BackoffPolicy, Job, JobApp, JobStatus, PermanentJobError, Worker, queries

FAST_RETRY = BackoffPolicy(base_seconds=0.01, max_seconds=0.05)


@asynccontextmanager
async def running_worker(app: JobApp, **options: Any) -> AsyncIterator[Worker]:
    settings: dict[str, Any] = {
        "poll_interval_seconds": 0.2,
        "reap_interval_seconds": 0.2,
        "lease_seconds": 5,
        "shutdown_grace_seconds": 5,
    }
    settings.update(options)
    worker = Worker(app, **settings)
    run = asyncio.create_task(worker.run())
    try:
        yield worker
    finally:
        worker.stop()
        await asyncio.wait_for(run, timeout=15)


async def wait_for_status(
    app: JobApp, job_id: int, status: JobStatus, timeout_seconds: float = 5
) -> Job:
    deadline = time.monotonic() + timeout_seconds
    while True:
        job = await app.get_job(job_id)
        assert job is not None
        if job.status == status:
            return job
        if time.monotonic() > deadline:
            raise AssertionError(f"job {job_id} is {job.status}, expected {status}")
        await asyncio.sleep(0.05)


async def test_successful_job(app: JobApp) -> None:
    received: list[dict[str, Any]] = []

    @app.task("collect")
    async def collect(job: Job) -> None:
        received.append(job.payload)

    result = await collect.enqueue({"n": 1})
    async with running_worker(app) as worker:
        job = await wait_for_status(app, result.job_id, JobStatus.SUCCEEDED)
    assert received == [{"n": 1}]
    assert job.attempts == 1 and job.finished_at is not None
    assert worker.stats.succeeded == 1


async def test_sync_handlers_are_supported(app: JobApp) -> None:
    @app.task("sync")
    def blocking(job: Job) -> None:
        time.sleep(0.05)

    result = await blocking.enqueue()
    async with running_worker(app):
        await wait_for_status(app, result.job_id, JobStatus.SUCCEEDED)


async def test_failed_job_is_retried_until_it_succeeds(app: JobApp) -> None:
    calls = 0

    @app.task("flaky", backoff=FAST_RETRY)
    async def flaky(job: Job) -> None:
        nonlocal calls
        calls += 1
        if calls < 3:
            raise ConnectionError("downstream unavailable")

    result = await flaky.enqueue()
    async with running_worker(app) as worker:
        job = await wait_for_status(app, result.job_id, JobStatus.SUCCEEDED)
    assert job.attempts == 3
    assert worker.stats.retried == 2


async def test_job_dies_after_max_attempts(app: JobApp) -> None:
    @app.task("always_fails", max_attempts=3, backoff=FAST_RETRY)
    async def always_fails(job: Job) -> None:
        raise ValueError("bad input")

    result = await always_fails.enqueue()
    async with running_worker(app):
        job = await wait_for_status(app, result.job_id, JobStatus.DEAD)
    assert job.attempts == 3
    assert job.last_error is not None and "ValueError: bad input" in job.last_error


async def test_permanent_error_skips_retries(app: JobApp) -> None:
    @app.task("permanent", max_attempts=5)
    async def permanent(job: Job) -> None:
        raise PermanentJobError("user was deleted")

    result = await permanent.enqueue()
    async with running_worker(app):
        job = await wait_for_status(app, result.job_id, JobStatus.DEAD)
    assert job.attempts == 1


async def test_unknown_task_goes_straight_to_dead(app: JobApp) -> None:
    result = await app.enqueue("nobody.handles.this")
    async with running_worker(app):
        job = await wait_for_status(app, result.job_id, JobStatus.DEAD)
    assert job.last_error is not None and "no handler registered" in job.last_error


async def test_timeout_counts_as_a_failure(app: JobApp) -> None:
    @app.task("slow", timeout_seconds=0.1, max_attempts=2, backoff=FAST_RETRY)
    async def slow(job: Job) -> None:
        await asyncio.sleep(10)

    result = await slow.enqueue()
    async with running_worker(app):
        job = await wait_for_status(app, result.job_id, JobStatus.DEAD)
    assert job.attempts == 2
    assert job.last_error is not None and "timed out" in job.last_error


async def test_heartbeat_keeps_long_jobs_alive(app: JobApp) -> None:
    @app.task("long")
    async def long_running(job: Job) -> None:
        await asyncio.sleep(1.5)

    result = await long_running.enqueue()
    # The lease (0.6s) is far shorter than the job (1.5s); heartbeats every
    # 0.2s must keep it alive so the reaper never steals it.
    async with running_worker(app, lease_seconds=0.6, reap_interval_seconds=0.1):
        job = await wait_for_status(app, result.job_id, JobStatus.SUCCEEDED)
    assert job.attempts == 1


async def test_notify_wakes_an_idle_worker_immediately(app: JobApp) -> None:
    @app.task("ping")
    async def ping(job: Job) -> None:
        pass

    async with running_worker(app, poll_interval_seconds=30, reap_interval_seconds=30):
        await asyncio.sleep(0.5)  # let the worker reach its idle wait
        started = time.monotonic()
        result = await ping.enqueue()
        await wait_for_status(app, result.job_id, JobStatus.SUCCEEDED, timeout_seconds=3)
    assert time.monotonic() - started < 3


async def test_concurrency_limit_is_respected(app: JobApp) -> None:
    running = 0
    peak = 0

    @app.task("tracked")
    async def tracked(job: Job) -> None:
        nonlocal running, peak
        running += 1
        peak = max(peak, running)
        await asyncio.sleep(0.05)
        running -= 1

    ids = [(await tracked.enqueue()).job_id for _ in range(30)]
    async with running_worker(app, concurrency=4):
        for job_id in ids:
            await wait_for_status(app, job_id, JobStatus.SUCCEEDED)
    assert peak == 4


async def test_graceful_shutdown_releases_unfinished_jobs(app: JobApp) -> None:
    started = asyncio.Event()

    @app.task("stuck", max_attempts=1)
    async def stuck(job: Job) -> None:
        started.set()
        await asyncio.sleep(60)

    result = await stuck.enqueue()
    async with running_worker(app, shutdown_grace_seconds=0.2) as worker:
        await asyncio.wait_for(started.wait(), 5)
    job = await app.get_job(result.job_id)
    assert job is not None
    assert job.status == JobStatus.QUEUED
    assert job.attempts == 0  # shutdown does not use up the only attempt
    assert worker.stats.released == 1


async def test_graceful_shutdown_lets_short_jobs_finish(app: JobApp) -> None:
    started = asyncio.Event()

    @app.task("short")
    async def short(job: Job) -> None:
        started.set()
        await asyncio.sleep(0.3)

    result = await short.enqueue()
    async with running_worker(app, shutdown_grace_seconds=5):
        await asyncio.wait_for(started.wait(), 5)
    job = await app.get_job(result.job_id)
    assert job is not None and job.status == JobStatus.SUCCEEDED


async def test_two_workers_share_the_load_without_duplicates(app: JobApp) -> None:
    executions: list[int] = []

    @app.task("shared")
    async def shared(job: Job) -> None:
        executions.append(job.id)
        await asyncio.sleep(0.01)

    ids = [(await shared.enqueue()).job_id for _ in range(100)]
    async with running_worker(app, concurrency=5), running_worker(app, concurrency=5):
        for job_id in ids:
            await wait_for_status(app, job_id, JobStatus.SUCCEEDED)
    assert sorted(executions) == sorted(ids)


async def test_retries_and_delayed_jobs_run_on_time_without_polling(app: JobApp) -> None:
    """Retries and delayed jobs send no NOTIFY; the worker must still wake on time."""
    calls = 0

    @app.task("retry_once", backoff=BackoffPolicy(base_seconds=0.3, max_seconds=0.3, jitter=False))
    async def retry_once(job: Job) -> None:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise ConnectionError("try again")

    @app.task("delayed")
    async def delayed(job: Job) -> None:
        pass

    # A 30s poll interval means only "sleep until next due job" can explain
    # these finishing within a couple of seconds.
    async with running_worker(app, poll_interval_seconds=30, reap_interval_seconds=30):
        await asyncio.sleep(0.3)
        started = time.monotonic()
        retried = await retry_once.enqueue()
        later = await delayed.enqueue(delay_seconds=1.0)
        await wait_for_status(app, retried.job_id, JobStatus.SUCCEEDED, timeout_seconds=3)
        await wait_for_status(app, later.job_id, JobStatus.SUCCEEDED, timeout_seconds=3)
        elapsed = time.monotonic() - started
    assert calls == 2
    assert 1.0 <= elapsed < 2.0


async def test_delayed_job_alone_wakes_an_idle_worker_on_time(app: JobApp) -> None:
    """Regression: a delayed job used to send no NOTIFY, so an idle worker
    only noticed it on its next poll."""

    @app.task("later")
    async def later(job: Job) -> None:
        pass

    async with running_worker(app, poll_interval_seconds=30, reap_interval_seconds=30):
        await asyncio.sleep(0.5)  # worker is idle, sleeping for up to 30s
        started = time.monotonic()
        result = await later.enqueue(delay_seconds=1.0)
        await wait_for_status(app, result.job_id, JobStatus.SUCCEEDED, timeout_seconds=4)
    assert 1.0 <= time.monotonic() - started < 2.0


async def test_reaped_jobs_wake_idle_peer_workers(app: JobApp) -> None:
    """Regression: a job recovered by one worker's reaper must wake idle peers,
    not wait for their poll interval."""
    busy = asyncio.Event()

    @app.task("hog")
    async def hog(job: Job) -> None:
        busy.set()
        await asyncio.sleep(4)

    @app.task("orphan")
    async def orphan(job: Job) -> None:
        pass

    # Worker A: full (concurrency 1) and reaping often, so it cannot run the
    # recovered job itself.
    hog_job = await hog.enqueue()
    async with running_worker(app, concurrency=1, reap_interval_seconds=0.1):
        await asyncio.wait_for(busy.wait(), 5)
        # Simulate a crashed worker holding "orphan" with a short lease.
        orphan_job = await orphan.enqueue()
        async with app.pool.connection() as conn:
            await queries.claim(
                conn, queues=["default"], limit=1, lease_seconds=0.3, worker_id="crashed"
            )
        # Worker B: idle, polls and reaps only every 30s; only a NOTIFY can wake it.
        async with running_worker(app, poll_interval_seconds=30, reap_interval_seconds=30):
            started = time.monotonic()
            await wait_for_status(app, orphan_job.job_id, JobStatus.SUCCEEDED, timeout_seconds=3)
            assert time.monotonic() - started < 2.0
    await wait_for_status(app, hog_job.job_id, JobStatus.SUCCEEDED)
