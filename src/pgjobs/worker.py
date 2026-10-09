"""The worker: claims jobs, runs handlers, and records outcomes.

Lifecycle of one job inside a worker::

    claim (lease + token) -> run handler  -> mark_succeeded
                              |  heartbeat extends the lease while running
                              |  exception -> schedule_retry or mark_dead
                              |  lease lost -> abandon (another worker owns it)
                              |  shutdown   -> release for immediate retry

Delivery guarantee: at-least-once. A handler can run more than once if a
worker dies or stalls after doing the work but before recording success, so
handlers must be idempotent. See docs/delivery-semantics.md.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import math
import os
import signal
import socket
import traceback
import uuid
from collections.abc import Sequence
from dataclasses import dataclass

import psycopg

from pgjobs import queries
from pgjobs.app import JobApp
from pgjobs.backoff import BackoffPolicy
from pgjobs.errors import ConfigurationError, PermanentJobError, summarize_error
from pgjobs.models import Job
from pgjobs.schema import NOTIFY_CHANNEL

logger = logging.getLogger("pgjobs.worker")

_LISTENER_RETRY_SECONDS = 5.0
# Lower bound on idle sleeps, so a due job that another worker is claiming
# right now does not make this worker spin.
_MIN_WAIT_SECONDS = 0.05


@dataclass(slots=True)
class WorkerStats:
    claimed: int = 0
    succeeded: int = 0
    retried: int = 0
    dead: int = 0
    lease_lost: int = 0
    released: int = 0


@dataclass(slots=True)
class _Execution:
    """Mutable state shared between a running job and its heartbeat."""

    job: Job
    lease_lost: bool = False


class Worker:
    def __init__(
        self,
        app: JobApp,
        *,
        queues: Sequence[str] = ("default",),
        concurrency: int = 10,
        lease_seconds: float = 30.0,
        poll_interval_seconds: float = 5.0,
        reap_interval_seconds: float = 10.0,
        shutdown_grace_seconds: float = 30.0,
        worker_id: str | None = None,
        use_listen_notify: bool = True,
    ) -> None:
        if not queues:
            raise ConfigurationError("at least one queue is required")
        if concurrency < 1:
            raise ConfigurationError("concurrency must be >= 1")
        # `not (x > 0)` rather than `x <= 0`: every comparison with NaN is
        # False, so NaN would slip through the latter.
        for option, value in [
            ("lease_seconds", lease_seconds),
            ("poll_interval_seconds", poll_interval_seconds),
            ("reap_interval_seconds", reap_interval_seconds),
        ]:
            if not (math.isfinite(value) and value > 0):
                raise ConfigurationError(f"{option} must be a positive, finite number")
        if not (math.isfinite(shutdown_grace_seconds) and shutdown_grace_seconds >= 0):
            raise ConfigurationError("shutdown_grace_seconds must be a finite number >= 0")

        self.app = app
        self.queues = list(dict.fromkeys(queues))
        self.concurrency = concurrency
        self.lease_seconds = lease_seconds
        self.poll_interval_seconds = poll_interval_seconds
        self.reap_interval_seconds = reap_interval_seconds
        self.shutdown_grace_seconds = shutdown_grace_seconds
        # Disable behind poolers in transaction mode (e.g. PgBouncer), which
        # do not support LISTEN; the worker then relies on polling alone.
        self.use_listen_notify = use_listen_notify
        self.worker_id = worker_id or f"{socket.gethostname()}:{os.getpid()}:{uuid.uuid4().hex[:8]}"
        self.stats = WorkerStats()

        self._active: set[asyncio.Task[None]] = set()
        self._wakeup = asyncio.Event()
        self._stop_requested = False

    # -- public API --------------------------------------------------------

    def stop(self) -> None:
        """Ask the worker to finish in-flight jobs and exit. Safe to call repeatedly."""
        if not self._stop_requested:
            logger.info("worker %s: shutdown requested", self.worker_id)
        self._stop_requested = True
        self._wakeup.set()

    def install_signal_handlers(self) -> None:
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, self.stop)

    async def run(self) -> None:
        logger.info(
            "worker %s: starting (queues=%s, concurrency=%d)",
            self.worker_id,
            ",".join(self.queues),
            self.concurrency,
        )
        listener = (
            asyncio.create_task(self._listen(), name="pgjobs-listener")
            if self.use_listen_notify
            else None
        )
        loop = asyncio.get_running_loop()
        next_reap_at = loop.time()
        try:
            while not self._stop_requested:
                # Clear before claiming: a notification or a finished job that
                # arrives while we claim leaves the event set, so the wait
                # below returns immediately instead of missing the wake-up.
                self._wakeup.clear()
                next_due: float | None = None
                try:
                    if loop.time() >= next_reap_at:
                        await self._reap()
                        next_reap_at = loop.time() + self.reap_interval_seconds
                    next_due = await self._claim_available()
                except (psycopg.Error, OSError) as exc:
                    logger.warning("worker %s: database error, will retry: %s", self.worker_id, exc)

                timeout = min(self.poll_interval_seconds, next_reap_at - loop.time())
                if next_due is not None:
                    timeout = min(timeout, next_due)
                await self._wait_for_wakeup(max(_MIN_WAIT_SECONDS, timeout))
        finally:
            if listener is not None:
                listener.cancel()
                await asyncio.gather(listener, return_exceptions=True)
            await self._drain()
            logger.info("worker %s: stopped (%s)", self.worker_id, self.stats)

    # -- main loop helpers -------------------------------------------------

    async def _claim_available(self) -> float | None:
        """Claim jobs into free slots.

        Returns the seconds until the next queued job is due when the queue
        was drained, or None when the worker is full (a finishing job wakes
        it) or nothing is queued (a NOTIFY wakes it).
        """
        free_slots = self.concurrency - len(self._active)
        if free_slots <= 0:
            return None
        async with self.app.pool.connection() as conn:
            jobs = await queries.claim(
                conn,
                queues=self.queues,
                limit=free_slots,
                lease_seconds=self.lease_seconds,
                worker_id=self.worker_id,
            )
            next_due = (
                await queries.seconds_until_next_due(conn, queues=self.queues)
                if len(jobs) < free_slots
                else None
            )
        for job in jobs:
            self.stats.claimed += 1
            task = asyncio.create_task(self._execute(job), name=f"pgjobs-job-{job.id}")
            self._active.add(task)
            task.add_done_callback(self._on_job_done)
        return next_due

    def _on_job_done(self, task: asyncio.Task[None]) -> None:
        self._active.discard(task)
        self._wakeup.set()
        if not task.cancelled() and (exc := task.exception()) is not None:
            logger.error("worker %s: unexpected error in job runner", self.worker_id, exc_info=exc)

    async def _wait_for_wakeup(self, max_wait_seconds: float) -> None:
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(self._wakeup.wait(), max_wait_seconds)

    async def _reap(self) -> None:
        async with self.app.pool.connection() as conn:
            reaped = await queries.reap_expired_leases(conn)
        for job_id, status in reaped:
            logger.warning("worker %s: job %d lease expired -> %s", self.worker_id, job_id, status)

    async def _listen(self) -> None:
        """Wake the main loop on NOTIFY. Polling remains the fallback if this fails."""
        while True:
            try:
                async with await psycopg.AsyncConnection.connect(
                    self.app.database_url, autocommit=True
                ) as conn:
                    await conn.execute(f"LISTEN {NOTIFY_CHANNEL}")
                    async for notify in conn.notifies():
                        if not notify.payload or notify.payload in self.queues:
                            self._wakeup.set()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.warning(
                    "worker %s: notification listener failed (%s); falling back to polling",
                    self.worker_id,
                    exc,
                )
                await asyncio.sleep(_LISTENER_RETRY_SECONDS)

    async def _drain(self) -> None:
        if not self._active:
            return
        logger.info(
            "worker %s: waiting up to %.0fs for %d running job(s)",
            self.worker_id,
            self.shutdown_grace_seconds,
            len(self._active),
        )
        _, pending = await asyncio.wait(set(self._active), timeout=self.shutdown_grace_seconds)
        for task in pending:
            task.cancel()
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)

    # -- running one job ---------------------------------------------------

    async def _execute(self, job: Job) -> None:
        task = self.app.registry.get(job.task_name)
        if task is None:
            await self._record_dead(job, f"no handler registered for task {job.task_name!r}")
            return

        execution = _Execution(job)
        handler = asyncio.create_task(task.run(job), name=f"pgjobs-handler-{job.id}")
        heartbeat = asyncio.create_task(self._heartbeat(execution, handler))
        try:
            async with asyncio.timeout(task.timeout_seconds):
                await handler
        except asyncio.CancelledError:
            current = asyncio.current_task()
            if current is not None and current.cancelling():
                # The worker is shutting down and the grace period ran out.
                await self._release(execution)
                raise
            if execution.lease_lost:
                self.stats.lease_lost += 1
                logger.warning(
                    "worker %s: job %d lost its lease; handler cancelled", self.worker_id, job.id
                )
                return
            await self._record_failure(execution, "handler was cancelled", task.backoff)
        except TimeoutError:
            await self._record_failure(
                execution, f"timed out after {task.timeout_seconds}s", task.backoff
            )
        except PermanentJobError as exc:
            await self._record_dead(job, _format_error(exc))
        except Exception as exc:
            await self._record_failure(execution, _format_error(exc), task.backoff)
        else:
            await self._record_success(execution)
        finally:
            heartbeat.cancel()
            await asyncio.gather(heartbeat, return_exceptions=True)

    async def _heartbeat(self, execution: _Execution, handler: asyncio.Task[None]) -> None:
        interval = self.lease_seconds / 3
        job = execution.job
        assert job.lease_token is not None
        while not handler.done():
            await asyncio.sleep(interval)
            try:
                async with self.app.pool.connection() as conn:
                    still_owned = await queries.extend_lease(
                        conn,
                        job_id=job.id,
                        lease_token=job.lease_token,
                        lease_seconds=self.lease_seconds,
                    )
            except (psycopg.Error, OSError) as exc:
                # Keep trying: the lease is still valid until it expires, and
                # the next successful heartbeat will tell us if it was lost.
                logger.warning(
                    "worker %s: heartbeat for job %d failed: %s", self.worker_id, job.id, exc
                )
                continue
            if not still_owned:
                execution.lease_lost = True
                handler.cancel()
                return

    async def _record_success(self, execution: _Execution) -> None:
        job = execution.job
        assert job.lease_token is not None
        async with self.app.pool.connection() as conn:
            recorded = await queries.mark_succeeded(
                conn, job_id=job.id, lease_token=job.lease_token
            )
        if recorded:
            self.stats.succeeded += 1
        else:
            # The work was done, but another worker may now be redoing it.
            self.stats.lease_lost += 1
            logger.warning(
                "worker %s: job %d finished after its lease was lost; it may run again",
                self.worker_id,
                job.id,
            )

    async def _record_failure(
        self, execution: _Execution, error: str, backoff: BackoffPolicy
    ) -> None:
        job = execution.job
        if job.is_final_attempt:
            await self._record_dead(job, error)
            return
        assert job.lease_token is not None
        delay = backoff.delay_for(job.attempts)
        async with self.app.pool.connection() as conn:
            recorded = await queries.schedule_retry(
                conn, job_id=job.id, lease_token=job.lease_token, delay_seconds=delay, error=error
            )
        if recorded:
            self.stats.retried += 1
            logger.info(
                "worker %s: job %d failed (attempt %d/%d), retrying in %.1fs: %s",
                self.worker_id,
                job.id,
                job.attempts,
                job.max_attempts,
                delay,
                summarize_error(error),
            )
        else:
            self.stats.lease_lost += 1

    async def _record_dead(self, job: Job, error: str) -> None:
        assert job.lease_token is not None
        async with self.app.pool.connection() as conn:
            recorded = await queries.mark_dead(
                conn, job_id=job.id, lease_token=job.lease_token, error=error
            )
        if recorded:
            self.stats.dead += 1
            logger.error(
                "worker %s: job %d is dead after %d attempt(s): %s",
                self.worker_id,
                job.id,
                job.attempts,
                summarize_error(error),
            )
        else:
            self.stats.lease_lost += 1

    async def _release(self, execution: _Execution) -> None:
        """Hand an interrupted job back to the queue without using up an attempt."""
        job = execution.job
        if execution.lease_lost or job.lease_token is None:
            return
        try:
            async with self.app.pool.connection() as conn:
                released = await queries.release(conn, job_id=job.id, lease_token=job.lease_token)
        except (psycopg.Error, OSError) as exc:
            # Not fatal: the lease will expire and the reaper will requeue it.
            logger.warning("worker %s: could not release job %d: %s", self.worker_id, job.id, exc)
            return
        if released:
            self.stats.released += 1


def _format_error(exc: BaseException) -> str:
    return "".join(traceback.format_exception(exc)).strip()
