"""Tests for the dashboard's JSON API and its security properties."""

from __future__ import annotations

from collections.abc import AsyncIterator

import httpx
import pytest

from pgjobs import JobApp, JobStatus, queries
from pgjobs.dashboard import create_dashboard
from pgjobs.dashboard.server import ACTION_HEADER


@pytest.fixture
async def client(app: JobApp) -> AsyncIterator[httpx.AsyncClient]:
    transport = httpx.ASGITransport(app=create_dashboard(app))
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as http:
        yield http


async def make_dead_job(app: JobApp, *, queue: str = "default", error: str = "boom") -> int:
    result = await app.enqueue("t", queue=queue, max_attempts=1)
    async with app.pool.connection() as conn:
        (job,) = await queries.claim(
            conn, queues=[queue], limit=1, lease_seconds=30, worker_id="test"
        )
        assert job.lease_token is not None
        await queries.mark_dead(conn, job_id=job.id, lease_token=job.lease_token, error=error)
    return result.job_id


async def test_page_and_assets_are_served_with_security_headers(
    client: httpx.AsyncClient,
) -> None:
    for path, content_type in [
        ("/", "text/html"),
        ("/static/dashboard.css", "text/css"),
        ("/static/dashboard.js", "text/javascript"),
    ]:
        response = await client.get(path)
        assert response.status_code == 200
        assert response.headers["content-type"].startswith(content_type)
        assert "default-src 'self'" in response.headers["content-security-policy"]
        assert response.headers["x-content-type-options"] == "nosniff"


async def test_summary_reports_each_queue(app: JobApp, client: httpx.AsyncClient) -> None:
    await app.enqueue("t", queue="emails")
    await app.enqueue("t", queue="emails", delay_seconds=600)
    await make_dead_job(app, queue="reports")

    response = await client.get("/api/summary")
    assert response.status_code == 200
    queues = {q["queue"]: q for q in response.json()["queues"]}
    assert queues["emails"]["ready"] == 1
    assert queues["emails"]["scheduled"] == 1
    assert queues["emails"]["oldest_ready_seconds"] is not None
    assert queues["reports"]["dead"] == 1
    assert queues["reports"]["died_recently"] == 1


async def test_jobs_can_be_filtered_by_status_and_queue(
    app: JobApp, client: httpx.AsyncClient
) -> None:
    queued = await app.enqueue("t", queue="a")
    dead = await make_dead_job(app, queue="b")

    all_jobs = (await client.get("/api/jobs")).json()["jobs"]
    assert [job["id"] for job in all_jobs] == [dead, queued.job_id]

    only_dead = (await client.get("/api/jobs", params={"status": "dead"})).json()["jobs"]
    assert [job["id"] for job in only_dead] == [dead]

    only_a = (await client.get("/api/jobs", params={"queue": "a"})).json()["jobs"]
    assert [job["id"] for job in only_a] == [queued.job_id]


async def test_bad_filters_are_rejected(client: httpx.AsyncClient) -> None:
    assert (await client.get("/api/jobs", params={"status": "bogus"})).status_code == 400
    assert (await client.get("/api/jobs", params={"limit": "x"})).status_code == 400
    assert (await client.get("/api/jobs/not-a-number")).status_code == 400
    assert (await client.get("/api/jobs/999999")).status_code == 404


async def test_job_detail_includes_payload_and_full_error(
    app: JobApp, client: httpx.AsyncClient
) -> None:
    traceback_text = "Traceback (most recent call last):\n  ...\nValueError: bad input"
    job_id = await make_dead_job(app, error=traceback_text)
    detail = (await client.get(f"/api/jobs/{job_id}")).json()
    assert detail["last_error"] == traceback_text
    assert detail["error_summary"] == "ValueError: bad input"
    assert detail["payload"] == {}


async def test_requeue_requires_the_action_header(app: JobApp, client: httpx.AsyncClient) -> None:
    await make_dead_job(app)
    # A cross-site form post cannot add custom headers, so it is refused.
    refused = await client.post("/api/requeue-dead")
    assert refused.status_code == 403
    accepted = await client.post("/api/requeue-dead", headers={ACTION_HEADER: "1"})
    assert accepted.json() == {"requeued": 1}


async def test_requeue_is_post_only(client: httpx.AsyncClient) -> None:
    assert (await client.get("/api/requeue-dead")).status_code == 405


async def test_read_only_dashboard_refuses_actions(app: JobApp) -> None:
    await make_dead_job(app)
    transport = httpx.ASGITransport(app=create_dashboard(app, allow_actions=False))
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as http:
        response = await http.post("/api/requeue-dead", headers={ACTION_HEADER: "1"})
        assert response.status_code == 403
        assert (await http.get("/api/summary")).json()["actions_enabled"] is False
    assert await app.stats() == {"default": {"dead": 1}}


async def test_dashboard_queries_use_indexes_not_full_scans(app: JobApp) -> None:
    """EXPLAIN the exact statements the dashboard runs, with 50,000 finished
    jobs in the table, and require that none of them scans the whole table.

    Scope: this checks the query plans on this data, not response times.
    """
    async with app.pool.connection() as conn:
        await conn.execute(
            """
            INSERT INTO pgjobs_jobs (task_name, status, finished_at, updated_at)
            SELECT 't', 'succeeded', now() - interval '1 day', now() - interval '1 day'
            FROM generate_series(1, 50000)
            """
        )
        await conn.execute("ANALYZE pgjobs_jobs")
        statements: list[tuple[str, dict[str, object]]] = [
            (queries.OVERVIEW_QUEUED_SQL, {}),
            (queries.OVERVIEW_RUNNING_SQL, {}),
            (queries.OVERVIEW_DEAD_SQL, {}),
            (queries.OVERVIEW_RECENT_SQL, {"recent_seconds": 300}),
        ]
        for status in (None, JobStatus.DEAD, JobStatus.SUCCEEDED):
            params = {"status": status.value if status else None, "queue": None, "limit": 50}
            statements.append((queries.recent_jobs_sql(status), params))

        for sql, params in statements:
            cur = await conn.execute(f"EXPLAIN {sql}", params or None)
            plan = "\n".join(row[0] for row in await cur.fetchall())
            assert "Seq Scan" not in plan, f"{sql}\n{plan}"
