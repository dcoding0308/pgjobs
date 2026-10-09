"""Shared fixtures.

Integration tests need a PostgreSQL database and read its URL from
``PGJOBS_TEST_DATABASE_URL``. They are skipped when it is not set, so the
unit tests still run anywhere. CI always sets it.

The tests truncate the jobs table, so never point this at a real database.
"""

from __future__ import annotations

import os
from collections.abc import AsyncIterator

import pytest

from pgjobs import JobApp

TEST_DATABASE_URL = os.environ.get("PGJOBS_TEST_DATABASE_URL")


@pytest.fixture
def database_url() -> str:
    if not TEST_DATABASE_URL:
        pytest.skip("PGJOBS_TEST_DATABASE_URL is not set")
    return TEST_DATABASE_URL


@pytest.fixture
async def app(database_url: str) -> AsyncIterator[JobApp]:
    job_app = JobApp(database_url, pool_max_size=20)
    async with job_app:
        await job_app.migrate()
        async with job_app.pool.connection() as conn:
            await conn.execute("TRUNCATE pgjobs_jobs RESTART IDENTITY")
        yield job_app
