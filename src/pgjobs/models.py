"""Data types shared across the library."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from typing import Any
from uuid import UUID


class JobStatus(StrEnum):
    QUEUED = "queued"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    DEAD = "dead"


@dataclass(frozen=True, slots=True)
class Job:
    """A snapshot of a job row, as seen by the worker that claimed it."""

    id: int
    queue: str
    task_name: str
    payload: dict[str, Any]
    status: JobStatus
    priority: int
    attempts: int
    max_attempts: int
    run_after: datetime
    lease_token: UUID | None
    locked_by: str | None
    dedupe_key: str | None
    last_error: str | None
    created_at: datetime
    finished_at: datetime | None

    @classmethod
    def from_row(cls, row: dict[str, Any]) -> Job:
        return cls(
            id=row["id"],
            queue=row["queue"],
            task_name=row["task_name"],
            payload=row["payload"],
            status=JobStatus(row["status"]),
            priority=row["priority"],
            attempts=row["attempts"],
            max_attempts=row["max_attempts"],
            run_after=row["run_after"],
            lease_token=row["lease_token"],
            locked_by=row["locked_by"],
            dedupe_key=row["dedupe_key"],
            last_error=row["last_error"],
            created_at=row["created_at"],
            finished_at=row["finished_at"],
        )

    @property
    def is_final_attempt(self) -> bool:
        return self.attempts >= self.max_attempts


@dataclass(frozen=True, slots=True)
class EnqueueResult:
    """Outcome of an enqueue call.

    ``created`` is False when a job with the same ``dedupe_key`` already
    existed; ``job_id`` then refers to that existing job.
    """

    job_id: int
    created: bool
