"""Retry policy: which failures are worth retrying, and when to retry.

Pure functions — no I/O — so the policy is unit-tested on its own.
"""

import enum
import random
from collections.abc import Callable

from sqlalchemy.exc import DBAPIError

from app.processing import ChecksumMismatchError, UnsupportedFormatError
from app.services.file_service import UnknownFileError
from app.storage import ObjectNotFoundError, StorageError


class ErrorKind(enum.StrEnum):
    RETRYABLE = "retryable"  # may succeed later: the dependency was unavailable
    PERMANENT = "permanent"  # will fail the same way every time


# Retrying these cannot change the result: the input itself is bad.
PERMANENT_ERRORS: tuple[type[BaseException], ...] = (
    ChecksumMismatchError,  # stored object is corrupted
    UnsupportedFormatError,  # not a text file
    ObjectNotFoundError,  # the object is gone (checked before StorageError!)
    UnknownFileError,  # the task points to a file row that does not exist
)

# Typical transient failures: a dependency is down, slow or restarting.
RETRYABLE_ERRORS: tuple[type[BaseException], ...] = (
    StorageError,  # MinIO unreachable / timed out / connection reset mid-read
    DBAPIError,  # PostgreSQL connection problems
    OSError,  # network-level errors (ConnectionRefusedError, ...)
    TimeoutError,
)


def classify(exc: BaseException) -> ErrorKind:
    """Decide whether retrying `exc` makes sense.

    Order matters: ObjectNotFoundError is a StorageError, so the permanent
    list is checked first. Anything unknown counts as retryable: retries are
    bounded by task_max_attempts, so the worst case is a few wasted attempts,
    whereas treating an unexpected transient error as permanent would fail a
    task that could have succeeded.
    """
    if isinstance(exc, PERMANENT_ERRORS):
        return ErrorKind.PERMANENT
    if isinstance(exc, RETRYABLE_ERRORS):
        return ErrorKind.RETRYABLE
    return ErrorKind.RETRYABLE


def backoff_delay(
    failed_attempt: int,
    *,
    base_s: float,
    factor: float,
    max_s: float,
    jitter: float,
    rand: Callable[[], float] = random.random,
) -> float:
    """Seconds to wait before the attempt after `failed_attempt` (1-based).

    Exponential: each failure multiplies the wait, so a long outage is probed
    less and less often (backpressure on the failing dependency). Capped at
    max_s. Jitter spreads retries of tasks that failed together over time, so
    they do not all hit the recovering dependency in the same instant.
    """
    delay = min(max_s, base_s * factor ** (failed_attempt - 1))
    return delay * (1 - jitter + 2 * jitter * rand())
