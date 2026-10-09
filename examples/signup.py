"""Example: send a welcome email after a user signs up.

Shows the main reason to keep jobs in PostgreSQL: the user row and the job are
written in one transaction, so there is never a user without a welcome job,
or a welcome job for a user whose signup was rolled back.

    export DATABASE_URL=postgresql://pgjobs:pgjobs@localhost:5433/pgjobs
    uv run python examples/signup.py setup
    uv run python examples/signup.py signup alice@example.com
    uv run pgjobs worker examples.signup:app
"""

from __future__ import annotations

import asyncio
import logging
import os
import sys

from pgjobs import Job, JobApp, PermanentJobError

app = JobApp(os.environ.get("DATABASE_URL", "postgresql://pgjobs:pgjobs@localhost:5433/pgjobs"))
logger = logging.getLogger("examples.signup")


@app.task("emails.welcome", max_attempts=5, timeout_seconds=30)
async def send_welcome_email(job: Job) -> None:
    user_id = job.payload["user_id"]
    async with app.pool.connection() as conn:
        cur = await conn.execute(
            "SELECT email, welcomed_at FROM example_users WHERE id = %s", (user_id,)
        )
        row = await cur.fetchone()
        if row is None:
            raise PermanentJobError(f"user {user_id} no longer exists")
        email, welcomed_at = row
        if welcomed_at is not None:
            # Idempotency: a previous attempt already sent it.
            logger.info("user %s was already welcomed; skipping", user_id)
            return
        logger.info("sending welcome email to %s (attempt %d)", email, job.attempts)
        await asyncio.sleep(0.2)  # stand-in for a call to an email provider
        await conn.execute("UPDATE example_users SET welcomed_at = now() WHERE id = %s", (user_id,))


async def setup() -> None:
    async with app:
        await app.migrate()
        async with app.pool.connection() as conn:
            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS example_users (
                    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                    email TEXT NOT NULL UNIQUE,
                    welcomed_at TIMESTAMPTZ
                )
                """
            )
    print("example schema ready")


async def signup(email: str) -> None:
    async with app, app.pool.connection() as conn, conn.transaction():
        cur = await conn.execute(
            "INSERT INTO example_users (email) VALUES (%s) RETURNING id", (email,)
        )
        row = await cur.fetchone()
        assert row is not None
        result = await send_welcome_email.enqueue(
            {"user_id": row[0]}, dedupe_key=f"welcome:{row[0]}", connection=conn
        )
    print(f"user {row[0]} created; welcome email queued as job {result.job_id}")


if __name__ == "__main__":
    command = sys.argv[1] if len(sys.argv) > 1 else ""
    if command == "setup":
        asyncio.run(setup())
    elif command == "signup" and len(sys.argv) == 3:
        asyncio.run(signup(sys.argv[2]))
    else:
        print(__doc__)
        sys.exit(2)
