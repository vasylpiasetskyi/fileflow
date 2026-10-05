"""Object storage (MinIO via the S3 API).

We talk plain S3 through aiobotocore, so the same code works against AWS S3.
The client holds an HTTP connection pool; one per process, created at startup.
"""

import contextlib
from collections.abc import AsyncIterable, AsyncIterator
from contextlib import AsyncExitStack

from aiobotocore.config import AioConfig
from aiobotocore.session import get_session
from botocore.exceptions import BotoCoreError, ClientError

from app.config import Settings


class StorageError(Exception):
    """The object store failed or is unreachable."""


async def rechunk(chunks: AsyncIterable[bytes], size: int) -> AsyncIterator[bytes]:
    """Regroup an arbitrary byte stream into pieces of exactly `size` bytes.

    The last piece may be shorter. At most ~`size` bytes are buffered, which is
    what keeps memory flat no matter how large the stream is.
    """
    buffer = bytearray()
    async for chunk in chunks:
        buffer += chunk
        while len(buffer) >= size:
            yield bytes(buffer[:size])
            del buffer[:size]
    if buffer:
        yield bytes(buffer)


class Storage:
    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._stack = AsyncExitStack()
        self._client = None

    @property
    def bucket(self) -> str:
        return self._settings.s3_bucket

    async def start(self) -> None:
        # Creating the client does not contact MinIO, so startup succeeds
        # even if MinIO is down. Short timeouts + few retries make an outage
        # visible quickly instead of hanging requests.
        session = get_session()
        self._client = await self._stack.enter_async_context(
            session.create_client(
                "s3",
                endpoint_url=self._settings.s3_endpoint_url,
                aws_access_key_id=self._settings.s3_access_key,
                aws_secret_access_key=self._settings.s3_secret_key,
                region_name=self._settings.s3_region,
                config=AioConfig(
                    connect_timeout=2,
                    read_timeout=10,
                    retries={"max_attempts": 1},
                ),
            )
        )

    async def close(self) -> None:
        await self._stack.aclose()
        self._client = None

    async def ping(self) -> None:
        # HeadBucket: cheap, and checks both connectivity and that the bucket exists.
        await self._client.head_bucket(Bucket=self.bucket)

    async def upload_stream(
        self, key: str, chunks: AsyncIterable[bytes], content_type: str
    ) -> None:
        """Stream bytes into `key` using an S3 multipart upload.

        The object becomes visible only on CompleteMultipartUpload, so readers
        never see a half-written object. On any failure (including the client
        disconnecting, which arrives as CancelledError) the upload is aborted;
        otherwise the already uploaded parts would linger in the bucket.
        """
        try:
            created = await self._client.create_multipart_upload(
                Bucket=self.bucket, Key=key, ContentType=content_type
            )
        except (BotoCoreError, ClientError) as exc:
            raise StorageError(f"cannot start upload of {key}: {exc}") from exc

        upload_id = created["UploadId"]
        parts: list[dict] = []
        try:
            async for part in rechunk(chunks, self._settings.s3_part_size_bytes):
                parts.append(await self._upload_part(key, upload_id, len(parts) + 1, part))
            if not parts:
                # A multipart upload needs at least one part; an empty file is
                # a single empty (and therefore last) part.
                parts.append(await self._upload_part(key, upload_id, 1, b""))
            await self._client.complete_multipart_upload(
                Bucket=self.bucket,
                Key=key,
                UploadId=upload_id,
                MultipartUpload={"Parts": parts},
            )
        except BaseException as exc:
            # Best effort: if storage is down the abort fails too, and the
            # incomplete upload has to be cleaned up later (lifecycle rule).
            with contextlib.suppress(Exception):
                await self._client.abort_multipart_upload(
                    Bucket=self.bucket, Key=key, UploadId=upload_id
                )
            if isinstance(exc, BotoCoreError | ClientError):
                raise StorageError(f"upload of {key} failed: {exc}") from exc
            raise

    async def _upload_part(self, key: str, upload_id: str, number: int, body: bytes) -> dict:
        resp = await self._client.upload_part(
            Bucket=self.bucket, Key=key, UploadId=upload_id, PartNumber=number, Body=body
        )
        return {"PartNumber": number, "ETag": resp["ETag"]}
