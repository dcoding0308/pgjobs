"""Exceptions raised by pgjobs."""


class PgJobsError(Exception):
    """Base class for all pgjobs errors."""


class ConfigurationError(PgJobsError):
    """Raised when the library is configured incorrectly."""


class DuplicateTaskError(ConfigurationError):
    """Raised when two handlers are registered under the same task name."""


class PermanentJobError(PgJobsError):
    """Raise from a task handler to fail the job immediately, without retrying.

    Use this for errors that will never succeed on a retry, such as invalid
    payloads or references to records that no longer exist.
    """


def summarize_error(error: str | None, max_length: int = 300) -> str | None:
    """Return the most useful line of an error, e.g. 'ValueError: bad input'.

    For a Python traceback that is the last non-empty line.
    """
    if not error:
        return None
    lines = [line.strip() for line in error.strip().splitlines() if line.strip()]
    return lines[-1][:max_length] if lines else None
