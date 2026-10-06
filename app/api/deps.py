"""FastAPI dependencies: the bridge between app.state and endpoints.

Endpoints never touch globals; they declare what they need. Tests replace any
of these with `app.dependency_overrides[...] = fake`.
"""

from collections.abc import Awaitable, Callable
from pathlib import Path

from fastapi import Request
from fastapi.templating import Jinja2Templates
from sqlalchemy.ext.asyncio import AsyncEngine

from app import db, kafka
from app.config import Settings
from app.services.file_service import FileService
from app.services.task_service import TaskService
from app.storage import Storage

Probe = Callable[[], Awaitable[None]]

templates = Jinja2Templates(directory=Path(__file__).resolve().parents[2] / "templates")


def get_settings(request: Request) -> Settings:
    return request.app.state.settings


def get_engine(request: Request) -> AsyncEngine:
    return request.app.state.engine


def get_storage(request: Request) -> Storage:
    return request.app.state.storage


def get_file_service(request: Request) -> FileService:
    # Cheap to construct: it only holds references to process-wide resources.
    return FileService(request.app.state.sessionmaker, get_storage(request))


def get_task_service(request: Request) -> TaskService:
    return TaskService(request.app.state.sessionmaker)


def get_health_probes(request: Request) -> dict[str, Probe]:
    """Named dependency probes. A probe returns on success and raises on failure."""
    settings = get_settings(request)
    engine = get_engine(request)
    storage = get_storage(request)
    return {
        "postgres": lambda: db.ping(engine),
        "minio": storage.ping,
        "kafka": lambda: kafka.ping(settings),
    }
