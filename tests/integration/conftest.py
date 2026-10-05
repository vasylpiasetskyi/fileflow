"""Fixtures for tests against the docker compose infrastructure.

Tests use the dev database and bucket; every test creates its own uniquely
identified rows/objects, so they do not interfere with each other.
"""

from collections.abc import AsyncIterator
from pathlib import Path

import httpx
import pytest
from aiobotocore.session import get_session
from alembic import command
from alembic.config import Config
from fastapi import FastAPI

from app.api.main import create_app
from app.config import get_settings

ROOT_DIR = Path(__file__).resolve().parents[2]


@pytest.fixture(scope="session", autouse=True)
def migrated_database() -> None:
    config = Config(ROOT_DIR / "alembic.ini")
    config.attributes["configure_logging"] = False  # keep pytest's log capture
    command.upgrade(config, "head")


@pytest.fixture
async def app() -> AsyncIterator[FastAPI]:
    app = create_app()
    # httpx.ASGITransport does not run the lifespan; run it explicitly so the
    # app gets real connection pools, exactly as under uvicorn.
    async with app.router.lifespan_context(app):
        yield app


@pytest.fixture
async def client(app: FastAPI) -> AsyncIterator[httpx.AsyncClient]:
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        yield client


@pytest.fixture
async def s3():
    """An independent S3 client, to inspect what really landed in the bucket."""
    settings = get_settings()
    async with get_session().create_client(
        "s3",
        endpoint_url=settings.s3_endpoint_url,
        aws_access_key_id=settings.s3_access_key,
        aws_secret_access_key=settings.s3_secret_key,
        region_name=settings.s3_region,
    ) as client:
        yield client
