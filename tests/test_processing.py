import hashlib
from collections.abc import AsyncIterator

import pytest

from app.processing import StreamAnalyzer, UnsupportedFormatError, analyze

SAMPLES = [
    b"",
    b"hello",
    b"hello world\n",
    b"one two\nthree\n\nfour",
    b"  leading and trailing  \n",
    b"tabs\tand\r\nwindows\r\nlines\x0bvt\x0cff",
    "юнікод працює теж\n".encode(),
]


def _expected(data: bytes) -> tuple[int, str, int, int]:
    return len(data), hashlib.sha256(data).hexdigest(), data.count(b"\n"), len(data.split())


def _feed(chunks: list[bytes]) -> tuple[int, str, int, int]:
    analyzer = StreamAnalyzer()
    for chunk in chunks:
        analyzer.feed(chunk)
    r = analyzer.result()
    return r.size, r.sha256, r.line_count, r.word_count


@pytest.mark.parametrize("data", SAMPLES)
def test_single_chunk_matches_whole_file_computation(data):
    assert _feed([data]) == _expected(data)


@pytest.mark.parametrize("data", SAMPLES)
def test_every_chunk_size_gives_the_same_result(data):
    # Word or line split across a chunk boundary must not change the counts.
    for size in range(1, len(data) + 1):
        chunks = [data[i : i + size] for i in range(0, len(data), size)]
        assert _feed(chunks) == _expected(data), f"chunk size {size}"


def test_word_cut_by_a_chunk_boundary_is_counted_once():
    assert _feed([b"hel", b"lo wor", b"ld"])[3] == 2


def test_empty_chunks_are_ignored():
    assert _feed([b"a", b"", b"b"]) == _expected(b"ab")


async def test_analyze_consumes_an_async_stream():
    async def stream() -> AsyncIterator[bytes]:
        yield b"one two\n"
        yield b"three"

    stats = await analyze(stream())

    assert (stats.size, stats.line_count, stats.word_count) == (13, 1, 3)


@pytest.mark.parametrize("chunks", [[b"text\x00more"], [b"plain text ", b"then \x00 binary"]])
def test_binary_content_is_rejected(chunks):
    with pytest.raises(UnsupportedFormatError):
        _feed(chunks)
