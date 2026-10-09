import pytest

from pgjobs import ConfigurationError, JobApp, Worker


@pytest.mark.parametrize(
    "option", ["lease_seconds", "poll_interval_seconds", "reap_interval_seconds"]
)
@pytest.mark.parametrize("value", [0, -1, float("nan"), float("inf")])
def test_invalid_intervals_are_rejected(option: str, value: float) -> None:
    # NaN slips past a plain `<= 0` check because every comparison with NaN is False.
    with pytest.raises(ConfigurationError):
        Worker(JobApp("postgresql://unused/unused"), **{option: value})


@pytest.mark.parametrize("value", [-1, float("nan"), float("inf")])
def test_invalid_shutdown_grace_is_rejected(value: float) -> None:
    with pytest.raises(ConfigurationError):
        Worker(JobApp("postgresql://unused/unused"), shutdown_grace_seconds=value)
