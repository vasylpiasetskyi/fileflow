import hashlib
from collections.abc import AsyncIterator

from app.services.file_service import StreamDigest
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
