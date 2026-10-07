import hashlib
from collections.abc import AsyncIterator

import pytest

from app.services.file_service import StreamDigest, UploadTooLargeError, limit_size
from app.storage import rechunk


async def _stream(*chunks: bytes) -> AsyncIterator[bytes]:
    for chunk in chunks:
        yield chunk


async def _collect(stream: AsyncIterator[bytes]) -> list[bytes]:
    return [chunk async for chunk in stream]


async def test_rechunk_regroups_small_chunks_into_fixed_size_parts():
    parts = await _collect(rechunk(_stream(b"ab", b"cde", b"f", b"ghij"), size=4))
    assert parts == [b"abcd", b"efgh", b"ij"]


async def test_rechunk_splits_a_chunk_larger_than_part_size():
    parts = await _collect(rechunk(_stream(b"0123456789"), size=3))
    assert parts == [b"012", b"345", b"678", b"9"]


async def test_rechunk_emits_nothing_for_an_empty_stream():
    assert await _collect(rechunk(_stream(), size=4)) == []


async def test_rechunk_has_no_trailing_empty_part_on_exact_multiple():
    parts = await _collect(rechunk(_stream(b"abcd", b"efgh"), size=4))
    assert parts == [b"abcd", b"efgh"]


async def test_stream_digest_passes_bytes_through_and_measures_them():
    digest = StreamDigest()
    passed = await _collect(digest.wrap(_stream(b"hello ", b"world")))

    assert b"".join(passed) == b"hello world"
    assert digest.size == 11
    assert digest.sha256 == hashlib.sha256(b"hello world").hexdigest()


async def test_limit_size_passes_a_stream_within_the_limit():
    assert await _collect(limit_size(_stream(b"abc", b"de"), max_bytes=5)) == [b"abc", b"de"]


async def test_limit_size_fails_as_soon_as_the_limit_is_crossed():
    seen = []
    with pytest.raises(UploadTooLargeError):
        async for chunk in limit_size(_stream(b"abc", b"def", b"never read"), max_bytes=5):
            seen.append(chunk)

    assert seen == [b"abc"]  # stopped on the 2nd chunk, the 3rd was never pulled
