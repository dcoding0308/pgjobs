"""Optional web dashboard. Install with ``pip install "pgjobs[dashboard]"``."""

from pgjobs.dashboard.server import create_dashboard

__all__ = ["create_dashboard"]
