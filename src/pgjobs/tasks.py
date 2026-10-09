"""Task definitions and the registry that maps task names to handlers."""

from __future__ import annotations

import asyncio
import inspect
import math
import re
from collections.abc import Awaitable, Callable, Iterator
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from pgjobs.backoff import BackoffPolicy
from pgjobs.errors import ConfigurationError, DuplicateTaskError
from pgjobs.models import EnqueueResult, Job

if TYPE_CHECKING:
    from pgjobs.app import JobApp

Handler = Callable[[Job], Awaitable[None]] | Callable[[Job], None]

_VALID_NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_.:-]{0,199}$")


@dataclass(frozen=True, slots=True)
class Task:
    """A registered handler plus its default execution settings."""

    name: str
    handler: Handler
    queue: str = "default"
    max_attempts: int = 5
    timeout_seconds: float | None = None
    backoff: BackoffPolicy = field(default_factory=BackoffPolicy)
    app: JobApp | None = field(default=None, repr=False, compare=False)

    def __post_init__(self) -> None:
        if not _VALID_NAME.match(self.name):
            raise ConfigurationError(f"invalid task name {self.name!r}")
        if self.max_attempts < 1:
            raise ConfigurationError("max_attempts must be >= 1")
        if self.timeout_seconds is not None and not (
            math.isfinite(self.timeout_seconds) and self.timeout_seconds > 0
        ):
            raise ConfigurationError("timeout_seconds must be a positive, finite number")

    async def run(self, job: Job) -> None:
        """Invoke the handler. Synchronous handlers run in a worker thread.

        A thread cannot be interrupted: after a timeout, a shutdown, or a
        lost lease, a synchronous handler keeps running (and causing side
        effects) while the job may be retried elsewhere. Prefer async
        handlers when cancellation matters.
        """
        if inspect.iscoroutinefunction(self.handler):
            await self.handler(job)
        else:
            await asyncio.to_thread(self.handler, job)

    async def enqueue(self, payload: dict[str, Any] | None = None, **options: Any) -> EnqueueResult:
        """Shortcut for ``app.enqueue(task.name, payload, ...)``."""
        if self.app is None:
            raise ConfigurationError(f"task {self.name!r} is not attached to a JobApp")
        return await self.app.enqueue(self.name, payload, **options)

    def __call__(self, job: Job) -> Awaitable[None] | None:
        """Call the handler directly, which is convenient in unit tests."""
        return self.handler(job)


class TaskRegistry:
    def __init__(self) -> None:
        self._tasks: dict[str, Task] = {}

    def register(self, task: Task) -> Task:
        if task.name in self._tasks:
            raise DuplicateTaskError(f"task {task.name!r} is already registered")
        self._tasks[task.name] = task
        return task

    def get(self, name: str) -> Task | None:
        return self._tasks.get(name)

    def __contains__(self, name: object) -> bool:
        return name in self._tasks

    def __iter__(self) -> Iterator[Task]:
        return iter(self._tasks.values())

    def __len__(self) -> int:
        return len(self._tasks)
