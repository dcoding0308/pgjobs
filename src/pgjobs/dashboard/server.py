"""A small monitoring dashboard: one HTML page backed by a JSON API.

Security model: the dashboard has no login, so it binds to localhost by
default and is meant to sit behind your own authentication if exposed. The
only state-changing endpoint (requeue dead jobs) requires a custom request
header. Browsers do not let another website add custom headers to a
cross-origin request without a CORS preflight, which this server never
approves, so a malicious page cannot trigger it (CSRF protection). It can
also be disabled entirely with ``allow_actions=False``.

All job data is rendered in the browser with ``textContent``, never as HTML,
so payloads and error messages cannot inject markup (XSS).
"""

from __future__ import annotations

from datetime import datetime
from importlib import resources
from typing import Any

from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse, Response
from starlette.routing import Route
from starlette.types import ASGIApp, Message, Receive, Scope, Send

from pgjobs import queries
from pgjobs.app import JobApp
from pgjobs.errors import summarize_error
from pgjobs.models import Job, JobStatus

ACTION_HEADER = "x-pgjobs-action"
DEFAULT_LIMIT = 50
MAX_LIMIT = 200
RECENT_SECONDS = 300

_STATIC_TYPES = {
    "index.html": "text/html; charset=utf-8",
    "dashboard.css": "text/css; charset=utf-8",
    "dashboard.js": "text/javascript; charset=utf-8",
}

_SECURITY_HEADERS = [
    (b"content-security-policy", b"default-src 'self'; frame-ancestors 'none'; base-uri 'none'"),
    (b"x-content-type-options", b"nosniff"),
    (b"referrer-policy", b"no-referrer"),
    (b"cache-control", b"no-store"),
]


def create_dashboard(app: JobApp, *, allow_actions: bool = True) -> Starlette:
    """Build the dashboard ASGI app. ``app`` must be opened by the caller."""
    static_files = {name: _read_static(name) for name in _STATIC_TYPES}

    def static(name: str) -> Any:
        async def endpoint(request: Request) -> Response:
            return Response(static_files[name], media_type=_STATIC_TYPES[name])

        return endpoint

    async def summary(request: Request) -> JSONResponse:
        async with app.pool.connection() as conn:
            overview = await queries.queue_overview(conn, recent_seconds=RECENT_SECONDS)
        return JSONResponse(
            {
                "queues": overview,
                "recent_window_seconds": RECENT_SECONDS,
                "actions_enabled": allow_actions,
            }
        )

    async def list_jobs(request: Request) -> JSONResponse:
        params = request.query_params
        try:
            status = JobStatus(params["status"]) if params.get("status") else None
            limit = int(params.get("limit", DEFAULT_LIMIT))
        except ValueError:
            return _error(400, "invalid status or limit")
        limit = max(1, min(limit, MAX_LIMIT))
        async with app.pool.connection() as conn:
            jobs = await queries.recent_jobs(
                conn, status=status, queue=params.get("queue") or None, limit=limit
            )
        return JSONResponse({"jobs": [_job_summary(job) for job in jobs]})

    async def job_detail(request: Request) -> JSONResponse:
        try:
            job_id = int(request.path_params["job_id"])
        except ValueError:
            return _error(400, "job id must be an integer")
        job = await app.get_job(job_id)
        if job is None:
            return _error(404, f"job {job_id} not found")
        return JSONResponse(
            {**_job_summary(job), "payload": job.payload, "last_error": job.last_error}
        )

    async def requeue_dead(request: Request) -> JSONResponse:
        if not allow_actions:
            return _error(403, "actions are disabled on this dashboard")
        if request.headers.get(ACTION_HEADER) != "1":
            return _error(403, f"missing {ACTION_HEADER} header")
        queue = request.query_params.get("queue") or None
        count = await app.requeue_dead(queue)
        return JSONResponse({"requeued": count})

    routes = [
        Route("/", static("index.html")),
        Route("/static/dashboard.css", static("dashboard.css")),
        Route("/static/dashboard.js", static("dashboard.js")),
        Route("/api/summary", summary),
        Route("/api/jobs", list_jobs),
        Route("/api/jobs/{job_id}", job_detail),
        Route("/api/requeue-dead", requeue_dead, methods=["POST"]),
    ]
    dashboard = Starlette(routes=routes)
    dashboard.add_middleware(SecurityHeadersMiddleware)
    return dashboard


class SecurityHeadersMiddleware:
    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        async def send_with_headers(message: Message) -> None:
            if message["type"] == "http.response.start":
                message["headers"] = [*message.get("headers", []), *_SECURITY_HEADERS]
            await send(message)

        await self.app(scope, receive, send_with_headers)


def _job_summary(job: Job) -> dict[str, Any]:
    return {
        "id": job.id,
        "queue": job.queue,
        "task_name": job.task_name,
        "status": job.status.value,
        "attempts": job.attempts,
        "max_attempts": job.max_attempts,
        "priority": job.priority,
        "locked_by": job.locked_by,
        "dedupe_key": job.dedupe_key,
        "created_at": _iso(job.created_at),
        "run_after": _iso(job.run_after),
        "finished_at": _iso(job.finished_at),
        "error_summary": summarize_error(job.last_error),
    }


def _iso(value: datetime | None) -> str | None:
    return value.isoformat() if value else None


def _error(status_code: int, message: str) -> JSONResponse:
    return JSONResponse({"error": message}, status_code=status_code)


def _read_static(name: str) -> bytes:
    return resources.files("pgjobs.dashboard").joinpath("static", name).read_bytes()
