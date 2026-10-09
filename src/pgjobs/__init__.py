"""pgjobs: a small, crash-tested background job queue built on PostgreSQL."""

from pgjobs.app import JobApp
from pgjobs.backoff import BackoffPolicy
from pgjobs.errors import ConfigurationError, DuplicateTaskError, PermanentJobError, PgJobsError
from pgjobs.models import EnqueueResult, Job, JobStatus
from pgjobs.tasks import Task
from pgjobs.worker import Worker, WorkerStats

__all__ = [
    "BackoffPolicy",
    "ConfigurationError",
    "DuplicateTaskError",
    "EnqueueResult",
    "Job",
    "JobApp",
    "JobStatus",
    "PermanentJobError",
    "PgJobsError",
    "Task",
    "Worker",
    "WorkerStats",
]

__version__ = "0.1.0"
