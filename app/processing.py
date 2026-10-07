"""File analysis: the (deliberately trivial) business logic of FileFlow.

Works on a stream of chunks, so memory use is one chunk no matter how large
the file is. Counting semantics follow `wc`:

- lines: number of b"\\n" bytes (a last line without newline is not counted)
- words: maximal runs of non-whitespace bytes (ASCII whitespace, like bytes.split())

Only text is supported: a NUL byte marks the file as binary and analysis stops
with UnsupportedFormatError (a permanent failure — retrying cannot help).
"""

import hashlib
from collections.abc import AsyncIterable
from dataclasses import dataclass


class UnsupportedFormatError(ValueError):
    """The file is not text (contains NUL bytes)."""


class ChecksumMismatchError(ValueError):
    """Stored bytes differ from what was uploaded: the object is corrupted."""


@dataclass(frozen=True)
class FileStats:
    size: int
    sha256: str
    line_count: int
    word_count: int


class StreamAnalyzer:
    """Incremental analyzer: feed() chunks in order, then read result()."""

    def __init__(self) -> None:
        self._sha256 = hashlib.sha256()
        self._size = 0
        self._lines = 0
        self._words = 0
        # Did the previous chunk end in the middle of a word? A word cut by a
        # chunk boundary ("hel" | "lo") must be counted once, not twice.
        self._in_word = False

    def feed(self, chunk: bytes) -> None:
        if not chunk:
            return
        if b"\x00" in chunk:
            raise UnsupportedFormatError(f"binary content (NUL byte near offset {self._size})")
        self._sha256.update(chunk)
        self._size += len(chunk)
        self._lines += chunk.count(b"\n")

        words = len(chunk.split())
        if self._in_word and not chunk[:1].isspace():
            words -= 1  # this chunk continues the word from the previous one
        self._words += words
        self._in_word = not chunk[-1:].isspace()

    def result(self) -> FileStats:
        return FileStats(
            size=self._size,
            sha256=self._sha256.hexdigest(),
            line_count=self._lines,
            word_count=self._words,
        )


async def analyze(chunks: AsyncIterable[bytes]) -> FileStats:
    analyzer = StreamAnalyzer()
    async for chunk in chunks:
        analyzer.feed(chunk)
    return analyzer.result()
