"""Object storage (MinIO via the S3 API).

We talk plain S3 through aiobotocore, so the same code works against AWS S3.
The client holds an HTTP connection pool; one per process, created at startup.
"""

from contextlib import AsyncExitStack

from aiobotocore.config import AioConfig
from aiobotocore.session import get_session

from app.config import Settings


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
