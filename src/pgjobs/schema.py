"""Database schema.

The schema is applied with idempotent ``IF NOT EXISTS`` statements inside a
transaction guarded by an advisory lock, so several processes can run the
migration at start-up without racing each other.
"""

from __future__ import annotations

MIGRATION_LOCK_ID = 7_342_001  # arbitrary, but must be stable across releases

NOTIFY_CHANNEL = "pgjobs"

SCHEMA_STATEMENTS: tuple[str, ...] = (
    """
    CREATE TABLE IF NOT EXISTS pgjobs_jobs (
        id            BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
        queue         TEXT        NOT NULL DEFAULT 'default',
        task_name     TEXT        NOT NULL,
        payload       JSONB       NOT NULL DEFAULT '{}'::jsonb,
        status        TEXT        NOT NULL DEFAULT 'queued'
                      CHECK (status IN ('queued', 'running', 'succeeded', 'dead')),
        priority      SMALLINT    NOT NULL DEFAULT 0,
        attempts      INTEGER     NOT NULL DEFAULT 0 CHECK (attempts >= 0),
        max_attempts  INTEGER     NOT NULL DEFAULT 5 CHECK (max_attempts >= 1),
        run_after     TIMESTAMPTZ NOT NULL DEFAULT now(),
        lease_token   UUID,
        locked_until  TIMESTAMPTZ,
        locked_by     TEXT,
        dedupe_key    TEXT,
        last_error    TEXT,
        created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
        updated_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
        finished_at   TIMESTAMPTZ,
        CONSTRAINT pgjobs_running_has_lease CHECK (
            status <> 'running' OR (lease_token IS NOT NULL AND locked_until IS NOT NULL)
        )
    )
    """,
    # Serves the claim query: only queued rows are indexed, so the index stays
    # small even when millions of finished jobs are retained.
    """
    CREATE INDEX IF NOT EXISTS pgjobs_jobs_claim_idx
        ON pgjobs_jobs (queue, priority DESC, run_after, id)
        WHERE status = 'queued'
    """,
    # Serves the reaper, which looks for running jobs whose lease has expired.
    """
    CREATE INDEX IF NOT EXISTS pgjobs_jobs_lease_idx
        ON pgjobs_jobs (locked_until)
        WHERE status = 'running'
    """,
    # Dashboard: dead jobs are rare, so this index is tiny but lets the
    # dashboard list and count them without scanning finished jobs.
    """
    CREATE INDEX IF NOT EXISTS pgjobs_jobs_dead_idx
        ON pgjobs_jobs (queue, updated_at DESC)
        WHERE status = 'dead'
    """,
    # Dashboard: recent completions ("done in the last 5 minutes"). Each job
    # enters this index once, when it finishes.
    """
    CREATE INDEX IF NOT EXISTS pgjobs_jobs_finished_idx
        ON pgjobs_jobs (finished_at)
        WHERE finished_at IS NOT NULL
    """,
    """
    CREATE UNIQUE INDEX IF NOT EXISTS pgjobs_jobs_dedupe_idx
        ON pgjobs_jobs (queue, dedupe_key)
        WHERE dedupe_key IS NOT NULL
    """,
)
