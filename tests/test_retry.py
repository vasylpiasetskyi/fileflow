import pytest
from sqlalchemy.exc import OperationalError

from app.processing import ChecksumMismatchError, UnsupportedFormatError
from app.retry import ErrorKind, backoff_delay, classify
from app.services.file_service import UnknownFileError
from app.storage import ObjectNotFoundError, StorageError


@pytest.mark.parametrize(
    ("exc", "kind"),
    [
        (StorageError("minio unreachable"), ErrorKind.RETRYABLE),
        (OperationalError("SELECT 1", {}, Exception("connection reset")), ErrorKind.RETRYABLE),
        (ConnectionRefusedError(), ErrorKind.RETRYABLE),
        (TimeoutError(), ErrorKind.RETRYABLE),
        (ChecksumMismatchError("corrupted"), ErrorKind.PERMANENT),
        (UnsupportedFormatError("binary"), ErrorKind.PERMANENT),
        (UnknownFileError(), ErrorKind.PERMANENT),
        # A StorageError subclass, but permanent: the more specific rule wins.
        (ObjectNotFoundError("files/x"), ErrorKind.PERMANENT),
        # Unknown errors are retried; task_max_attempts bounds the cost.
        (KeyError("surprise"), ErrorKind.RETRYABLE),
    ],
    ids=lambda v: type(v).__name__ if isinstance(v, BaseException) else str(v),
)
def test_classify(exc, kind):
    assert classify(exc) is kind


def _delay(attempt: int, rand: float = 0.5, **overrides) -> float:
    params = {"base_s": 5, "factor": 5, "max_s": 900, "jitter": 0.2} | overrides
    return backoff_delay(attempt, rand=lambda: rand, **params)


def test_backoff_grows_exponentially_without_jitter():
    # rand=0.5 is the midpoint of the jitter range, i.e. no jitter.
    assert [_delay(n) for n in (1, 2, 3, 4)] == [5, 25, 125, 625]


def test_backoff_is_capped():
    assert _delay(5) == 900  # 5 * 5^4 = 3125 -> capped
    assert _delay(50) == 900  # no overflow, still capped


@pytest.mark.parametrize(("rand", "expected"), [(0.0, 20.0), (1.0, 30.0)])
def test_jitter_spreads_the_delay_by_plus_minus_20_percent(rand, expected):
    assert _delay(2, rand=rand) == pytest.approx(expected)


def test_zero_jitter_is_deterministic():
    assert _delay(2, rand=0.0, jitter=0) == _delay(2, rand=1.0, jitter=0) == 25
