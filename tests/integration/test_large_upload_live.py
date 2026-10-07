"""Streaming upload (raw body) and upload size limits."""

import hashlib
from collections.abc import AsyncIterator

import pytest
from fastapi import FastAPI
from sqlalchemy import func, select

from app.config import get_settings
from app.models import File, FileStatus, Task

pytestmark = pytest.mark.integration


async def _body(total: int, chunk: int = 64 * 1024) -> AsyncIterator[bytes]:
    """A deterministic body sent with chunked transfer encoding (no Content-Length)."""
    pattern = b"abcdefghij\n" * (chunk // 11 + 1)
    sent = 0
    while sent < total:
        piece = pattern[: min(chunk, total - sent)]
        sent += len(piece)
        yield piece


def _limit(app: FastAPI, max_bytes: int) -> None:
    app.state.settings = app.state.settings.model_copy(update={"max_upload_bytes": max_bytes})


async def _files(app: FastAPI) -> list[File]:
    async with app.state.sessionmaker() as session:
        return list((await session.scalars(select(File))).all())


async def _object(s3, key: str) -> bytes:
    obj = await s3.get_object(Bucket=get_settings().s3_bucket, Key=key)
    async with obj["Body"] as body:
        return await body.read()


async def test_stream_upload_stores_the_raw_body(client, s3):
    content = b"line one\nline two\n"

    resp = await client.post(
        "/files/stream",
        params={"filename": "raw.txt"},
        content=content,
        headers={"Content-Type": "text/plain"},
    )

    assert resp.status_code == 201, resp.text
    body = resp.json()
    assert body["file"]["filename"] == "raw.txt"
    assert body["file"]["content_type"] == "text/plain"
    assert body["file"]["checksum_sha256"] == hashlib.sha256(content).hexdigest()
    assert body["task"]["status"] == "PENDING"
    assert await _object(s3, body["file"]["storage_key"]) == content


async def test_chunked_body_of_unknown_length_is_streamed_into_several_parts(client, s3):
    total = 20 * 1024 * 1024  # > two 8 MiB S3 parts
    expected = b"".join([piece async for piece in _body(total)])

    resp = await client.post("/files/stream", params={"filename": "big.txt"}, content=_body(total))

    assert resp.status_code == 201, resp.text
    assert resp.json()["file"]["size"] == total
    assert resp.json()["file"]["checksum_sha256"] == hashlib.sha256(expected).hexdigest()


async def test_declared_oversize_is_rejected_before_anything_is_stored(app, client):
    _limit(app, 10)

    resp = await client.post("/files/stream", params={"filename": "x"}, content=b"x" * 11)

    assert resp.status_code == 413
    assert await _files(app) == []  # not even an UPLOADING row


async def test_undeclared_oversize_is_cut_off_and_cleaned_up(app, client, s3):
    _limit(app, 1024 * 1024)

    resp = await client.post(
        "/files/stream", params={"filename": "liar.txt"}, content=_body(5 * 1024 * 1024)
    )

    assert resp.status_code == 413
    [file] = await _files(app)
    assert file.status is FileStatus.FAILED
    async with app.state.sessionmaker() as session:
        assert await session.scalar(select(func.count()).select_from(Task)) == 0
    uploads = await s3.list_multipart_uploads(
        Bucket=get_settings().s3_bucket, Prefix=file.storage_key
    )
    assert uploads.get("Uploads", []) == []  # the multipart upload was aborted


async def test_multipart_uploads_obey_the_same_limit(app, client):
    _limit(app, 10)

    resp = await client.post("/files", files={"file": ("f.txt", b"x" * 11)})

    assert resp.status_code == 413
    [file] = await _files(app)
    assert file.status is FileStatus.FAILED
