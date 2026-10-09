"""All SQL executed by pgjobs.

Every function takes an open connection and runs inside whatever transaction
that connection is in; committing is the caller's responsibility. This lets
callers enqueue a job atomically with their own business writes.

Correctness rules enforced here:

* Claiming uses ``FOR UPDATE SKIP LOCKED`` so concurrent workers never claim
  the same row and never block on each other.
* Every claim issues a fresh ``lease_token``. Any later state change for a
  running job (heartbeat, complete, retry, dead) must present that token. A
  worker whose lease expired and was reclaimed can therefore no longer change
  the job: the token acts as a fencing token.
* All timestamps come from the database clock (``now()``), so workers on
  machines with skewed clocks still agree on lease expiry.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any
from uuid import UUID

from psycopg import AsyncConnection
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from pgjobs.models import EnqueueResult, Job, JobStatus
from pgjobs.schema import MIGRATION_LOCK_ID, NOTIFY_CHANNEL, SCHEMA_STATEMENTS

AsyncConn = AsyncConnection[Any]

_JOB_COLUMNS = """
    id, queue, task_name, payload, status, priority, attempts, max_attempts,
    run_after, lease_token, locked_by, dedupe_key, last_error, created_at, finished_at
"""

# Postgres TEXT columns are unbounded, but huge tracebacks bloat the table.
MAX_ERROR_LENGTH = 4000


async def migrate(conn: AsyncConn) -> None:
    async with conn.transaction():
        await conn.execute("SELECT pg_advisory_xact_lock(%s)", (MIGRATION_LOCK_ID,))
        for statement in SCHEMA_STATEMENTS:
            await conn.execute(statement)


async def enqueue(
    conn: AsyncConn,
    *,
    task_name: str,
    payload: dict[str, Any],
    queue: str,
    priority: int,
    delay_seconds: float,
    max_attempts: int,
    dedupe_key: str | None,
) -> EnqueueResult:
    async with conn.cursor() as cur:
        await cur.execute(
            """
            INSERT INTO pgjobs_jobs
                (queue, task_name, payload, priority, max_attempts, run_after, dedupe_key)
            VALUES (%s, %s, %s, %s, %s, now() + make_interval(secs => %s), %s)
            ON CONFLICT (queue, dedupe_key) WHERE dedupe_key IS NOT NULL DO NOTHING
            RETURNING id
            """,
            (queue, task_name, Jsonb(payload), priority, max_attempts, delay_seconds, dedupe_key),
        )
        row = await cur.fetchone()
        if row is not None:
            job_id: int = row[0]
            # Delivered only when the surrounding transaction commits, so
            # workers are never woken for a job they cannot see yet. Delayed
            # jobs notify too: an idle worker sleeping until a later job is
            # due must recalculate its wake-up time.
            await cur.execute("SELECT pg_notify(%s, %s)", (NOTIFY_CHANNEL, queue))
            return EnqueueResult(job_id=job_id, created=True)

        await cur.execute(
            "SELECT id FROM pgjobs_jobs WHERE queue = %s AND dedupe_key = %s",
            (queue, dedupe_key),
        )
        existing = await cur.fetchone()
        if existing is None:  # pragma: no cover - conflict implies the row exists
            raise RuntimeError(f"dedupe conflict on {dedupe_key!r} but no job found")
        return EnqueueResult(job_id=existing[0], created=False)


async def claim(
    conn: AsyncConn,
    *,
    queues: Sequence[str],
    limit: int,
    lease_seconds: float,
    worker_id: str,
) -> list[Job]:
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            f"""
            WITH next_jobs AS (
                SELECT id
                FROM pgjobs_jobs
                WHERE status = 'queued'
                  AND queue = ANY(%s)
                  AND run_after <= now()
                ORDER BY priority DESC, run_after, id
                LIMIT %s
                FOR UPDATE SKIP LOCKED
            )
            UPDATE pgjobs_jobs AS j
            SET status       = 'running',
                attempts     = j.attempts + 1,
                lease_token  = gen_random_uuid(),
                locked_until = now() + make_interval(secs => %s),
                locked_by    = %s,
                updated_at   = now()
            FROM next_jobs
            WHERE j.id = next_jobs.id
            RETURNING {_qualified_columns("j")}
            """,
            (list(queues), limit, lease_seconds, worker_id),
        )
        rows = await cur.fetchall()
    jobs = [Job.from_row(row) for row in rows]
    # RETURNING order is unspecified; keep the priority order for callers.
    jobs.sort(key=lambda job: (-job.priority, job.run_after, job.id))
    return jobs


async def seconds_until_next_due(conn: AsyncConn, *, queues: Sequence[str]) -> float | None:
    """Seconds until the earliest queued job becomes due (<= 0 if one is due now).

    Retries and delayed jobs send no NOTIFY when they become due, so an idle
    worker uses this to sleep exactly until the next one instead of waiting
    for its poll interval.
    """
    async with conn.cursor() as cur:
        await cur.execute(
            """
            SELECT extract(epoch FROM min(run_after) - now())
            FROM pgjobs_jobs
            WHERE status = 'queued' AND queue = ANY(%s)
            """,
            (list(queues),),
        )
        row = await cur.fetchone()
    return float(row[0]) if row and row[0] is not None else None


async def extend_lease(
    conn: AsyncConn, *, job_id: int, lease_token: UUID, lease_seconds: float
) -> bool:
    cur = await conn.execute(
        """
        UPDATE pgjobs_jobs
        SET locked_until = now() + make_interval(secs => %s), updated_at = now()
        WHERE id = %s AND lease_token = %s AND status = 'running'
        """,
        (lease_seconds, job_id, lease_token),
    )
    return cur.rowcount == 1


async def mark_succeeded(conn: AsyncConn, *, job_id: int, lease_token: UUID) -> bool:
    cur = await conn.execute(
        """
        UPDATE pgjobs_jobs
        SET status = 'succeeded', lease_token = NULL, locked_until = NULL,
            last_error = NULL, finished_at = now(), updated_at = now()
        WHERE id = %s AND lease_token = %s AND status = 'running'
        """,
        (job_id, lease_token),
    )
    return cur.rowcount == 1


async def schedule_retry(
    conn: AsyncConn, *, job_id: int, lease_token: UUID, delay_seconds: float, error: str
) -> bool:
    cur = await conn.execute(
        """
        UPDATE pgjobs_jobs
        SET status = 'queued', lease_token = NULL, locked_until = NULL, locked_by = NULL,
            run_after = now() + make_interval(secs => %s),
            last_error = %s, updated_at = now()
        WHERE id = %s AND lease_token = %s AND status = 'running'
        RETURNING queue
        """,
        (delay_seconds, _truncate(error), job_id, lease_token),
    )
    row = await cur.fetchone()
    if row is None:
        return False
    # Wake peers so an idle one can schedule its wake-up for this retry; the
    # worker recording the failure may be too busy to run it on time.
    await conn.execute("SELECT pg_notify(%s, %s)", (NOTIFY_CHANNEL, row[0]))
    return True


async def mark_dead(conn: AsyncConn, *, job_id: int, lease_token: UUID, error: str) -> bool:
    cur = await conn.execute(
        """
        UPDATE pgjobs_jobs
        SET status = 'dead', lease_token = NULL, locked_until = NULL,
            last_error = %s, finished_at = now(), updated_at = now()
        WHERE id = %s AND lease_token = %s AND status = 'running'
        """,
        (_truncate(error), job_id, lease_token),
    )
    return cur.rowcount == 1


async def release(conn: AsyncConn, *, job_id: int, lease_token: UUID) -> bool:
    """Return a job to the queue after a graceful shutdown.

    The attempt is refunded because the job did not fail. Crashed workers are
    handled by the reaper instead, which does *not* refund the attempt: a job
    that crashes its worker every time (a "poison" job) must eventually die.
    """
    cur = await conn.execute(
        """
        UPDATE pgjobs_jobs
        SET status = 'queued', attempts = GREATEST(attempts - 1, 0),
            lease_token = NULL, locked_until = NULL, locked_by = NULL,
            run_after = now(), last_error = 'interrupted by worker shutdown',
            updated_at = now()
        WHERE id = %s AND lease_token = %s AND status = 'running'
        """,
        (job_id, lease_token),
    )
    if cur.rowcount == 1:
        await conn.execute("SELECT pg_notify(%s, '')", (NOTIFY_CHANNEL,))
    return cur.rowcount == 1


async def reap_expired_leases(conn: AsyncConn) -> list[tuple[int, str]]:
    """Return expired running jobs to the queue, or bury them if out of attempts.

    ``attempts`` was already incremented when the job was claimed, so a job
    whose worker died on its final attempt goes straight to ``dead``.
    Returns ``(job_id, new_status)`` pairs.
    """
    async with conn.cursor() as cur:
        await cur.execute(
            """
            WITH expired AS (
                SELECT id
                FROM pgjobs_jobs
                WHERE status = 'running' AND locked_until < now()
                FOR UPDATE SKIP LOCKED
            )
            UPDATE pgjobs_jobs AS j
            SET status       = CASE WHEN j.attempts >= j.max_attempts THEN 'dead' ELSE 'queued' END,
                finished_at  = CASE WHEN j.attempts >= j.max_attempts THEN now() END,
                run_after    = now(),
                last_error   = 'lease expired; worker ' || coalesce(j.locked_by, 'unknown')
                               || ' presumed dead',
                lease_token  = NULL,
                locked_until = NULL,
                locked_by    = NULL,
                updated_at   = now()
            FROM expired
            WHERE j.id = expired.id
            RETURNING j.id, j.status
            """
        )
        reaped = [(row[0], row[1]) for row in await cur.fetchall()]
        if any(status == "queued" for _, status in reaped):
            # The reaping worker may be full; let idle peers pick these up.
            await cur.execute("SELECT pg_notify(%s, '')", (NOTIFY_CHANNEL,))
        return reaped


async def get_job(conn: AsyncConn, job_id: int) -> Job | None:
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(f"SELECT {_JOB_COLUMNS} FROM pgjobs_jobs WHERE id = %s", (job_id,))
        row = await cur.fetchone()
    return Job.from_row(row) if row else None


async def counts_by_status(conn: AsyncConn) -> dict[str, dict[str, int]]:
    async with conn.cursor() as cur:
        await cur.execute(
            "SELECT queue, status, count(*) FROM pgjobs_jobs GROUP BY queue, status ORDER BY queue"
        )
        result: dict[str, dict[str, int]] = {}
        for queue, status, count in await cur.fetchall():
            result.setdefault(queue, {})[status] = count
        return result


# Dashboard statements. Each is served by a partial index, so its cost depends
# on the number of active, dead, and recently finished jobs, not on table size.
# tests/integration/test_dashboard.py runs EXPLAIN on these exact statements.
OVERVIEW_QUEUED_SQL = """
    SELECT queue,
           count(*) FILTER (WHERE run_after <= now()),
           count(*) FILTER (WHERE run_after > now()),
           extract(epoch FROM now() - min(run_after) FILTER (WHERE run_after <= now()))
    FROM pgjobs_jobs WHERE status = 'queued' GROUP BY queue
"""
OVERVIEW_RUNNING_SQL = """
    SELECT queue, count(*) FROM pgjobs_jobs WHERE status = 'running' GROUP BY queue
"""
OVERVIEW_DEAD_SQL = """
    SELECT queue, count(*) FROM pgjobs_jobs WHERE status = 'dead' GROUP BY queue
"""
OVERVIEW_RECENT_SQL = """
    SELECT queue,
           count(*) FILTER (WHERE status = 'succeeded'),
           count(*) FILTER (WHERE status = 'dead')
    FROM pgjobs_jobs
    WHERE finished_at IS NOT NULL
      AND finished_at > now() - make_interval(secs => %(recent_seconds)s)
    GROUP BY queue
"""


async def queue_overview(conn: AsyncConn, *, recent_seconds: float = 300) -> list[dict[str, Any]]:
    """Per-queue numbers for the dashboard.

    Unlike ``counts_by_status``, this never counts succeeded jobs in full.
    """
    overview: dict[str, dict[str, Any]] = {}

    def entry(queue: str) -> dict[str, Any]:
        return overview.setdefault(
            queue,
            {
                "queue": queue,
                "ready": 0,
                "scheduled": 0,
                "running": 0,
                "dead": 0,
                "succeeded_recently": 0,
                "died_recently": 0,
                "oldest_ready_seconds": None,
            },
        )

    async with conn.cursor() as cur:
        await cur.execute(OVERVIEW_QUEUED_SQL)
        for queue, ready, scheduled, oldest in await cur.fetchall():
            row = entry(queue)
            row["ready"], row["scheduled"] = ready, scheduled
            row["oldest_ready_seconds"] = float(oldest) if oldest is not None else None

        await cur.execute(OVERVIEW_RUNNING_SQL)
        for queue, running in await cur.fetchall():
            entry(queue)["running"] = running

        await cur.execute(OVERVIEW_DEAD_SQL)
        for queue, dead in await cur.fetchall():
            entry(queue)["dead"] = dead

        await cur.execute(OVERVIEW_RECENT_SQL, {"recent_seconds": recent_seconds})
        for queue, succeeded, died in await cur.fetchall():
            row = entry(queue)
            row["succeeded_recently"], row["died_recently"] = succeeded, died

    return [overview[queue] for queue in sorted(overview)]


def recent_jobs_sql(status: JobStatus | None) -> str:
    """Dead jobs are ordered by when they died (served by the dead-job index);
    everything else by id, which walks the primary key backwards."""
    order = "updated_at DESC, id DESC" if status == JobStatus.DEAD else "id DESC"
    return f"""
        SELECT {_JOB_COLUMNS} FROM pgjobs_jobs
        WHERE (%(status)s::text IS NULL OR status = %(status)s)
          AND (%(queue)s::text IS NULL OR queue = %(queue)s)
        ORDER BY {order}
        LIMIT %(limit)s
    """


async def recent_jobs(
    conn: AsyncConn, *, status: JobStatus | None, queue: str | None, limit: int
) -> list[Job]:
    """Most recent jobs, newest first, optionally filtered."""
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            recent_jobs_sql(status),
            {"status": status.value if status else None, "queue": queue, "limit": limit},
        )
        return [Job.from_row(row) for row in await cur.fetchall()]


async def requeue_dead(conn: AsyncConn, *, queue: str | None) -> int:
    """Give dead jobs a fresh set of attempts. Returns the number requeued."""
    cur = await conn.execute(
        """
        UPDATE pgjobs_jobs
        SET status = 'queued', attempts = 0, run_after = now(),
            finished_at = NULL, updated_at = now()
        WHERE status = 'dead' AND (%s::text IS NULL OR queue = %s)
        """,
        (queue, queue),
    )
    if cur.rowcount:
        await conn.execute("SELECT pg_notify(%s, %s)", (NOTIFY_CHANNEL, queue or ""))
    return cur.rowcount


def _qualified_columns(alias: str) -> str:
    return ", ".join(f"{alias}.{column.strip()}" for column in _JOB_COLUMNS.split(","))


def _truncate(text: str) -> str:
    if len(text) <= MAX_ERROR_LENGTH:
        return text
    return text[: MAX_ERROR_LENGTH - 15] + "... [truncated]"
