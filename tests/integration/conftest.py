"""Fixtures for tests against the docker compose infrastructure.

Integration tests use their own database, `<dev db>_test`, emptied before
every test: a worker claims *any* PENDING task, so tests cannot share tables
with dev data or with each other. The bucket is shared; object keys are
unique per file, so that is harmless.
"""

import asyncio
import os
from collections.abc import AsyncIterator
from pathlib import Path

import asyncpg
import httpx
import pytest
from aiobotocore.session import get_session
from alembic import command
from alembic.config import Config
from fastapi import FastAPI
from sqlalchemy import text
from sqlalchemy.engine import make_url

from app.api.main import create_app
from app.config import get_settings
from app.db import create_engine

ROOT_DIR = Path(__file__).resolve().parents[2]

# Point the whole test session at the test database *before* anything reads
# settings (get_settings() is cached).
_dev_url = make_url(get_settings().database_url)
TEST_DATABASE_URL = _dev_url.set(database=f"{_dev_url.database}_test")
os.environ["FILEFLOW_DATABASE_URL"] = TEST_DATABASE_URL.render_as_string(hide_password=False)
get_settings.cache_clear()


async def _create_database_if_missing() -> None:
    # CREATE DATABASE cannot run inside a transaction or against the database
    # being created, so use a plain asyncpg connection to the `postgres` db.
    conn = await asyncpg.connect(
        host=TEST_DATABASE_URL.host,
        port=TEST_DATABASE_URL.port,
        user=TEST_DATABASE_URL.username,
        password=TEST_DATABASE_URL.password,
        database="postgres",
    )
    try:
        exists = await conn.fetchval(
            "SELECT 1 FROM pg_database WHERE datname = $1", TEST_DATABASE_URL.database
        )
        if not exists:
            await conn.execute(f'CREATE DATABASE "{TEST_DATABASE_URL.database}"')
    finally:
        await conn.close()


@pytest.fixture(scope="session", autouse=True)
def migrated_database() -> None:
    asyncio.run(_create_database_if_missing())
    config = Config(ROOT_DIR / "alembic.ini")
    config.attributes["configure_logging"] = False  # keep pytest's log capture
    command.upgrade(config, "head")


@pytest.fixture(autouse=True)
async def clean_tables(migrated_database) -> None:
    engine = create_engine(get_settings())
    async with engine.begin() as conn:
        await conn.execute(text("TRUNCATE task_results, tasks, files"))
    await engine.dispose()


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
