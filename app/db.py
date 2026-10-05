"""PostgreSQL access: one async engine (connection pool) per process."""

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker, create_async_engine

from app.config import Settings


def create_engine(settings: Settings) -> AsyncEngine:
    # create_async_engine does NOT connect. Connections are opened lazily on
    # first use, so the API can start while PostgreSQL is still down.
    # pool_pre_ping: test a pooled connection before handing it out, so a
    # connection killed by a PostgreSQL restart is replaced instead of failing
    # the request.
    return create_async_engine(
        settings.database_url,
        pool_size=settings.db_pool_size,
        pool_pre_ping=True,
    )


def create_sessionmaker(engine: AsyncEngine) -> async_sessionmaker:
    # expire_on_commit=False: objects stay readable after commit without an
    # implicit (async-unsafe) refresh query.
    return async_sessionmaker(engine, expire_on_commit=False)


async def ping(engine: AsyncEngine) -> None:
    async with engine.connect() as conn:
        await conn.execute(text("SELECT 1"))
