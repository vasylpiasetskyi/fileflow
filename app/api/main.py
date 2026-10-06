"""FastAPI application: REST API + server-rendered UI.

Run locally:  uv run uvicorn app.api.main:app --reload
"""

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

from app.api.routes import files, health, tasks, ui
from app.config import get_settings
from app.db import create_engine, create_sessionmaker
from app.storage import Storage

ROOT_DIR = Path(__file__).resolve().parents[2]


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    # Everything before `yield` runs once at startup, everything after it at
    # shutdown. Long-lived resources (connection pools, clients) live on
    # app.state and reach endpoints through dependencies in app/api/deps.py.
    settings = get_settings()

    engine = create_engine(settings)
    storage = Storage(settings)
    await storage.start()

    app.state.settings = settings
    app.state.engine = engine
    app.state.sessionmaker = create_sessionmaker(engine)
    app.state.storage = storage
    try:
        yield
    finally:
        # Close in reverse order of creation.
        await storage.close()
        await engine.dispose()


def create_app() -> FastAPI:
    app = FastAPI(title="FileFlow", lifespan=lifespan)
    app.mount("/static", StaticFiles(directory=ROOT_DIR / "static"), name="static")
    app.include_router(health.router)
    app.include_router(files.router)
    app.include_router(tasks.router)
    app.include_router(ui.router)
    return app


app = create_app()
