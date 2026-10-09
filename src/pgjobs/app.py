"""The application object: owns the connection pool and the task registry."""

from __future__ import annotations

import json
import math
from collections.abc import Callable
from types import TracebackType
from typing import Any

from psycopg_pool import AsyncConnectionPool

from pgjobs import queries
from pgjobs.backoff import BackoffPolicy
from pgjobs.errors import ConfigurationError
from pgjobs.models import EnqueueResult, Job
from pgjobs.queries import AsyncConn
from pgjobs.tasks import Handler, Task, TaskRegistry

DEFAULT_QUEUE = "default"
DEFAULT_MAX_ATTEMPTS = 5
_PRIORITY_RANGE = range(-32768, 32768)  # SMALLINT


class JobApp:
    """Entry point for producers and workers.

    Typical use::

        app = JobApp("postgresql://localhost/app")

        @app.task(max_attempts=3)
        async def send_welcome_email(job: Job) -> None:
            ...

        async with app:
            await send_welcome_email.enqueue({"user_id": 42})
    """

    def __init__(
        self, database_url: str, *, pool_min_size: int = 1, pool_max_size: int = 10
    ) -> None:
        if not database_url:
            raise ConfigurationError("database_url is required")
        self.database_url = database_url
        self.registry = TaskRegistry()
        self._pool = AsyncConnectionPool(
            database_url, min_size=pool_min_size, max_size=pool_max_size, open=False
        )
        self._is_open = False

    # -- lifecycle ---------------------------------------------------------

    async def open(self, timeout_seconds: float = 10.0) -> None:
        if not self._is_open:
            await self._pool.open(wait=True, timeout=timeout_seconds)
            self._is_open = True

    async def close(self) -> None:
        if self._is_open:
            await self._pool.close()
            self._is_open = False

    async def __aenter__(self) -> JobApp:
        await self.open()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        await self.close()

    @property
    def pool(self) -> AsyncConnectionPool:
        if not self._is_open:
            raise ConfigurationError(
                "JobApp is not open; use 'async with app:' or 'await app.open()'"
            )
        return self._pool

    # -- task registration -------------------------------------------------

    def task(
        self,
        name: str | None = None,
        *,
        queue: str = DEFAULT_QUEUE,
        max_attempts: int = DEFAULT_MAX_ATTEMPTS,
        timeout_seconds: float | None = None,
        backoff: BackoffPolicy | None = None,
    ) -> Callable[[Handler], Task]:
        """Decorator that registers a handler as a task.

        The task name defaults to ``module.function``. Names are stored in
        the database, so renaming a function strands jobs already enqueued
        under the old name; pass an explicit ``name`` for long-lived tasks.
        """

        def decorator(handler: Handler) -> Task:
            task_name = name or f"{handler.__module__}.{handler.__qualname__}"
            return self.registry.register(
                Task(
                    name=task_name,
                    handler=handler,
                    queue=queue,
                    max_attempts=max_attempts,
                    timeout_seconds=timeout_seconds,
                    backoff=backoff or BackoffPolicy(),
                    app=self,
                )
            )

        return decorator

    # -- producer API ------------------------------------------------------

    async def enqueue(
        self,
        task_name: str,
        payload: dict[str, Any] | None = None,
        *,
        queue: str | None = None,
        priority: int = 0,
        delay_seconds: float = 0.0,
        max_attempts: int | None = None,
        dedupe_key: str | None = None,
        connection: AsyncConn | None = None,
    ) -> EnqueueResult:
        """Add a job to the queue.

        Pass ``connection`` to enqueue inside your own transaction: the job
        then becomes visible only if that transaction commits, which removes
        the "row saved but job lost" (or vice versa) failure mode.

        The task does not have to be registered in this process, so a web
        app can enqueue work handled by a separate worker codebase.
        """
        payload = {} if payload is None else payload
        _validate_payload(payload)
        if priority not in _PRIORITY_RANGE:
            raise ValueError("priority must fit in a SMALLINT (-32768..32767)")
        if not math.isfinite(delay_seconds) or delay_seconds < 0:
            raise ValueError("delay_seconds must be a finite number >= 0")

        task = self.registry.get(task_name)
        # Compare with None rather than relying on truthiness: an explicit 0 or
        # "" is a caller error, not a request for the default.
        default_queue = task.queue if task else DEFAULT_QUEUE
        default_attempts = task.max_attempts if task else DEFAULT_MAX_ATTEMPTS
        resolved_queue = queue if queue is not None else default_queue
        resolved_attempts = max_attempts if max_attempts is not None else default_attempts
        if not resolved_queue:
            raise ValueError("queue must be a non-empty string")
        if resolved_attempts < 1:
            raise ValueError("max_attempts must be >= 1")

        params: dict[str, Any] = {
            "task_name": task_name,
            "payload": payload,
            "queue": resolved_queue,
            "priority": priority,
            "delay_seconds": delay_seconds,
            "max_attempts": resolved_attempts,
            "dedupe_key": dedupe_key,
        }
        if connection is not None:
            return await queries.enqueue(connection, **params)
        async with self.pool.connection() as conn:
            return await queries.enqueue(conn, **params)

    # -- administration ----------------------------------------------------

    async def migrate(self) -> None:
        async with self.pool.connection() as conn:
            await queries.migrate(conn)

    async def get_job(self, job_id: int) -> Job | None:
        async with self.pool.connection() as conn:
            return await queries.get_job(conn, job_id)

    async def stats(self) -> dict[str, dict[str, int]]:
        async with self.pool.connection() as conn:
            return await queries.counts_by_status(conn)

    async def requeue_dead(self, queue: str | None = None) -> int:
        async with self.pool.connection() as conn:
            return await queries.requeue_dead(conn, queue=queue)


def _validate_payload(payload: dict[str, Any]) -> None:
    if not isinstance(payload, dict):
        raise TypeError("payload must be a dict")
    try:
        json.dumps(payload, allow_nan=False)
    except (TypeError, ValueError) as exc:
        raise TypeError(f"payload is not JSON-serialisable: {exc}") from exc
