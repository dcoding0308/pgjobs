"""Tests for the SQL layer: enqueue, claim, fencing, and lease expiry."""

from __future__ import annotations

import asyncio
import uuid

from pgjobs import Job, JobApp, JobStatus, queries


async def claim(
    app: JobApp, limit: int = 10, lease_seconds: float = 30, queue: str = "default"
) -> list[Job]:
    async with app.pool.connection() as conn:
        return await queries.claim(
            conn, queues=[queue], limit=limit, lease_seconds=lease_seconds, worker_id="test"
        )


async def test_enqueue_stores_the_job(app: JobApp) -> None:
    result = await app.enqueue("emails.send", {"to": "a@example.com"}, priority=3)
    job = await app.get_job(result.job_id)
    assert result.created
    assert job is not None
    assert (job.task_name, job.payload, job.status, job.priority, job.attempts) == (
        "emails.send",
        {"to": "a@example.com"},
        JobStatus.QUEUED,
        3,
        0,
    )


async def test_dedupe_key_makes_enqueue_idempotent(app: JobApp) -> None:
    first = await app.enqueue("report", {"day": "2026-01-01"}, dedupe_key="report:2026-01-01")
    second = await app.enqueue("report", {"day": "2026-01-01"}, dedupe_key="report:2026-01-01")
    assert first.created and not second.created
    assert first.job_id == second.job_id
    other_queue = await app.enqueue("report", queue="other", dedupe_key="report:2026-01-01")
    assert other_queue.created


async def test_enqueue_joins_the_callers_transaction(app: JobApp) -> None:
    async with app.pool.connection() as conn, conn.transaction(force_rollback=True):
        result = await app.enqueue("rolled.back", connection=conn)
    assert await app.get_job(result.job_id) is None


async def test_claim_orders_by_priority_then_age(app: JobApp) -> None:
    low = await app.enqueue("t", priority=0)
    high = await app.enqueue("t", priority=10)
    low_later = await app.enqueue("t", priority=0)
    jobs = await claim(app)
    assert [job.id for job in jobs] == [high.job_id, low.job_id, low_later.job_id]
    assert all(job.status == JobStatus.RUNNING and job.attempts == 1 for job in jobs)
    assert len({job.lease_token for job in jobs}) == 3


async def test_delayed_jobs_are_not_claimed_early(app: JobApp) -> None:
    await app.enqueue("t", delay_seconds=60)
    assert await claim(app) == []


async def test_claim_only_reads_requested_queues(app: JobApp) -> None:
    await app.enqueue("t", queue="emails")
    assert await claim(app, queue="default") == []
    assert len(await claim(app, queue="emails")) == 1


async def test_concurrent_claims_never_return_the_same_job(app: JobApp) -> None:
    for _ in range(200):
        await app.enqueue("t")
    batches = await asyncio.gather(*(claim(app, limit=15) for _ in range(20)))
    claimed_ids = [job.id for batch in batches for job in batch]
    assert len(claimed_ids) == 200
    assert len(set(claimed_ids)) == 200


async def test_stale_lease_token_cannot_change_the_job(app: JobApp) -> None:
    await app.enqueue("t")
    (job,) = await claim(app, lease_seconds=0.2)
    await asyncio.sleep(0.3)
    async with app.pool.connection() as conn:
        assert await queries.reap_expired_leases(conn) == [(job.id, "queued")]
    (reclaimed,) = await claim(app)
    assert reclaimed.id == job.id and reclaimed.attempts == 2
    assert job.lease_token is not None
    async with app.pool.connection() as conn:
        # The first worker comes back to life and tries to finish the job.
        assert not await queries.mark_succeeded(conn, job_id=job.id, lease_token=job.lease_token)
        assert not await queries.extend_lease(
            conn, job_id=job.id, lease_token=job.lease_token, lease_seconds=30
        )
        assert not await queries.mark_succeeded(conn, job_id=job.id, lease_token=uuid.uuid4())
        assert reclaimed.lease_token is not None
        assert await queries.mark_succeeded(conn, job_id=job.id, lease_token=reclaimed.lease_token)


async def test_reaper_buries_jobs_that_crash_on_their_last_attempt(app: JobApp) -> None:
    await app.enqueue("t", max_attempts=1)
    (job,) = await claim(app, lease_seconds=0.1)
    await asyncio.sleep(0.2)
    async with app.pool.connection() as conn:
        assert await queries.reap_expired_leases(conn) == [(job.id, "dead")]
    dead = await app.get_job(job.id)
    assert dead is not None and dead.status == JobStatus.DEAD
    assert dead.last_error is not None and "lease expired" in dead.last_error


async def test_reaper_ignores_live_leases(app: JobApp) -> None:
    await app.enqueue("t")
    await claim(app, lease_seconds=30)
    async with app.pool.connection() as conn:
        assert await queries.reap_expired_leases(conn) == []


async def test_release_refunds_the_attempt(app: JobApp) -> None:
    await app.enqueue("t", max_attempts=1)
    (job,) = await claim(app)
    assert job.lease_token is not None
    async with app.pool.connection() as conn:
        assert await queries.release(conn, job_id=job.id, lease_token=job.lease_token)
    (again,) = await claim(app)
    assert again.attempts == 1


async def test_requeue_dead_and_stats(app: JobApp) -> None:
    await app.enqueue("t", max_attempts=1)
    await app.enqueue("t")
    (job, _) = await claim(app)
    assert job.lease_token is not None
    async with app.pool.connection() as conn:
        await queries.mark_dead(conn, job_id=job.id, lease_token=job.lease_token, error="boom")
    assert await app.stats() == {"default": {"dead": 1, "running": 1}}
    assert await app.requeue_dead() == 1
    assert await app.stats() == {"default": {"queued": 1, "running": 1}}


async def test_long_errors_are_truncated(app: JobApp) -> None:
    await app.enqueue("t")
    (job,) = await claim(app)
    assert job.lease_token is not None
    async with app.pool.connection() as conn:
        await queries.mark_dead(
            conn, job_id=job.id, lease_token=job.lease_token, error="x" * 10_000
        )
    stored = await app.get_job(job.id)
    assert stored is not None and stored.last_error is not None
    assert len(stored.last_error) == queries.MAX_ERROR_LENGTH


async def test_migrate_is_idempotent_under_concurrency(app: JobApp) -> None:
    await asyncio.gather(*(app.migrate() for _ in range(5)))


async def test_lease_deadline_triggers_the_reaper_but_the_token_decides_ownership(
    app: JobApp,
) -> None:
    """Documents a deliberate design decision (see docs/delivery-semantics.md).

    A heartbeat that arrives after ``locked_until`` but before any reaper ran
    still renews: no other worker can own the job yet, so abandoning it would
    only cause a needless re-run. Once a reaper has requeued the job, the old
    token is worthless.
    """
    await app.enqueue("t")
    (job,) = await claim(app, lease_seconds=0.2)
    assert job.lease_token is not None
    await asyncio.sleep(0.3)  # deadline passed, nobody reaped yet

    async with app.pool.connection() as conn:
        assert await queries.extend_lease(
            conn, job_id=job.id, lease_token=job.lease_token, lease_seconds=0.2
        )
        assert await queries.reap_expired_leases(conn) == []  # renewed, so not reaped

    await asyncio.sleep(0.3)
    async with app.pool.connection() as conn:
        assert await queries.reap_expired_leases(conn) == [(job.id, "queued")]
        assert not await queries.extend_lease(
            conn, job_id=job.id, lease_token=job.lease_token, lease_seconds=30
        )


async def test_race_reaper_locks_first_then_heartbeat_loses(app: JobApp) -> None:
    """Heartbeat and reaper hit the same expired job; the reaper gets the row lock first.

    The heartbeat's UPDATE must wait for the lock, then re-check its WHERE
    clause against the reaper's committed row (READ COMMITTED), find that its
    lease_token no longer matches, and update nothing.
    """
    await app.enqueue("t")
    (job,) = await claim(app, lease_seconds=0.1)
    assert job.lease_token is not None
    await asyncio.sleep(0.2)

    async def heartbeat() -> bool:
        async with app.pool.connection() as conn:
            assert job.lease_token is not None
            return await queries.extend_lease(
                conn, job_id=job.id, lease_token=job.lease_token, lease_seconds=30
            )

    async with app.pool.connection() as reaper_conn, reaper_conn.transaction():
        assert await queries.reap_expired_leases(reaper_conn) == [(job.id, "queued")]
        pending_heartbeat = asyncio.create_task(heartbeat())
        await asyncio.sleep(0.3)
        assert not pending_heartbeat.done(), "heartbeat should be blocked on the row lock"
        # The reaper's transaction commits here, releasing the lock.

    assert await asyncio.wait_for(pending_heartbeat, 5) is False
    stored = await app.get_job(job.id)
    assert stored is not None
    assert (stored.status, stored.lease_token) == (JobStatus.QUEUED, None)


async def test_race_heartbeat_locks_first_then_reaper_skips(app: JobApp) -> None:
    """Same race, opposite order: the heartbeat holds the row lock.

    The reaper uses SKIP LOCKED, so it does not wait and does not touch the
    row. Once the heartbeat commits, the lease is in the future and later
    reaper passes leave the job alone.
    """
    await app.enqueue("t")
    (job,) = await claim(app, lease_seconds=0.1)
    assert job.lease_token is not None
    await asyncio.sleep(0.2)

    async with app.pool.connection() as heartbeat_conn, heartbeat_conn.transaction():
        assert await queries.extend_lease(
            heartbeat_conn, job_id=job.id, lease_token=job.lease_token, lease_seconds=30
        )
        async with app.pool.connection() as reaper_conn:
            # Returns immediately (no waiting) and reclaims nothing.
            reaped = await asyncio.wait_for(queries.reap_expired_leases(reaper_conn), 1)
            assert reaped == []

    async with app.pool.connection() as conn:
        assert await queries.reap_expired_leases(conn) == []
    stored = await app.get_job(job.id)
    assert stored is not None
    assert (stored.status, stored.lease_token) == (JobStatus.RUNNING, job.lease_token)
