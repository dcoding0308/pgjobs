import random

import pytest

from pgjobs.backoff import BackoffPolicy


def test_ceiling_doubles_each_attempt_until_capped() -> None:
    policy = BackoffPolicy(base_seconds=1, max_seconds=10)
    assert [policy.ceiling_for(n) for n in range(1, 7)] == [1, 2, 4, 8, 10, 10]


def test_without_jitter_delay_equals_ceiling() -> None:
    policy = BackoffPolicy(base_seconds=3, max_seconds=100, jitter=False)
    assert policy.delay_for(3) == 12


def test_jittered_delay_stays_within_bounds() -> None:
    policy = BackoffPolicy(base_seconds=2, max_seconds=60)
    rng = random.Random(1234)
    for attempt in range(1, 20):
        delay = policy.delay_for(attempt, rng)
        assert 0 <= delay <= policy.ceiling_for(attempt)


def test_jitter_spreads_retries_apart() -> None:
    policy = BackoffPolicy(base_seconds=10, max_seconds=10)
    rng = random.Random(7)
    delays = {round(policy.delay_for(1, rng), 3) for _ in range(50)}
    assert len(delays) > 40


def test_huge_attempt_numbers_do_not_overflow() -> None:
    assert BackoffPolicy(base_seconds=1, max_seconds=60).ceiling_for(10_000) == 60


@pytest.mark.parametrize(
    "kwargs",
    [{"base_seconds": -1}, {"base_seconds": 10, "max_seconds": 5}],
)
def test_invalid_policies_are_rejected(kwargs: dict[str, float]) -> None:
    with pytest.raises(ValueError):
        BackoffPolicy(**kwargs)


def test_attempt_must_be_positive() -> None:
    with pytest.raises(ValueError):
        BackoffPolicy().delay_for(0)


@pytest.mark.parametrize("value", [float("nan"), float("inf")])
def test_non_finite_values_are_rejected(value: float) -> None:
    with pytest.raises(ValueError):
        BackoffPolicy(base_seconds=1, max_seconds=value)
    with pytest.raises(ValueError):
        BackoffPolicy(base_seconds=value, max_seconds=10)
