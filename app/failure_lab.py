"""Fault injection for the Failure Lab: crash a process at a precise point.

Configured per process through settings (e.g. FILEFLOW_OUTBOX_CRASH_POINT).
Never set in normal operation.
"""

import logging
import os

log = logging.getLogger(__name__)


def crash_point(configured: str, point: str) -> None:
    """Kill the process right here if `point` is the configured crash point.

    os._exit skips finally blocks, context managers and connection cleanup —
    the closest in-process equivalent of `kill -9`. Open database transactions
    are rolled back by PostgreSQL when it sees the connection drop.
    """
    if configured == point:
        log.critical("FAILURE LAB: crashing at %s", point)
        os._exit(1)
