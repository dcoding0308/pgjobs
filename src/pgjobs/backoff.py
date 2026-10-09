"""Retry delay calculation."""

from __future__ import annotations

import math
import random
from dataclasses import dataclass

# 2**62 seconds is far beyond any sensible cap; clamping the exponent avoids
# float overflow for jobs configured with very large max_attempts.
_MAX_EXPONENT = 62


@dataclass(frozen=True, slots=True)
class BackoffPolicy:
    """Exponential backoff with optional "full jitter".

    The delay before retry ``n`` (1-based) is drawn uniformly from
    ``[0, min(max_seconds, base_seconds * 2 ** (n - 1))]``. Randomising the
    whole interval spreads retries from many failing jobs apart, so a
    recovering downstream service is not hit by a synchronised burst.
    """

    base_seconds: float = 2.0
    max_seconds: float = 300.0
    jitter: bool = True

    def __post_init__(self) -> None:
        if not (math.isfinite(self.base_seconds) and math.isfinite(self.max_seconds)):
            raise ValueError("base_seconds and max_seconds must be finite")
        if self.base_seconds < 0:
            raise ValueError("base_seconds must be >= 0")
        if self.max_seconds < self.base_seconds:
            raise ValueError("max_seconds must be >= base_seconds")

    def ceiling_for(self, attempt: int) -> float:
        """Return the maximum delay for the given attempt number."""
        if attempt < 1:
            raise ValueError("attempt must be >= 1")
        exponent = min(attempt - 1, _MAX_EXPONENT)
        return min(self.max_seconds, self.base_seconds * (2.0**exponent))

    def delay_for(self, attempt: int, rng: random.Random | None = None) -> float:
        """Return the delay in seconds before retrying after ``attempt`` failed."""
        ceiling = self.ceiling_for(attempt)
        if not self.jitter:
            return ceiling
        return (rng or random).uniform(0.0, ceiling)
