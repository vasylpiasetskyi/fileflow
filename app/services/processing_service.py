"""Processing use case: take a task, analyze its file, record the outcome."""

import logging
import time
import uuid
from contextlib import aclosing

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.config import Settings
from app.models import Task
from app.processing import analyze
from app.services.file_service import FileService
from app.services.task_service import TaskService
from app.storage import Storage

log = logging.getLogger(__name__)


class ChecksumMismatchError(Exception):
    """Stored bytes differ from what was uploaded."""


class ProcessingService:
    def __init__(
        self,
        sessionmaker: async_sessionmaker[AsyncSession],
        storage: Storage,
        settings: Settings,
    ) -> None:
        self._tasks = TaskService(sessionmaker)
        self._files = FileService(sessionmaker, storage)
        self._storage = storage
        self._settings = settings

    async def process_next(self) -> Task | None:
        """Claim and process one task. Returns None when there was nothing to do."""
        task = await self._tasks.claim_next(self._settings.worker_id)
        if task is None:
            return None
        return await self._process(task.id, task.file_id)

    async def _process(self, task_id: uuid.UUID, file_id: uuid.UUID) -> Task:
        log.info("task %s: processing file %s", task_id, file_id)
        started = time.perf_counter()
        try:
            file = await self._files.get(file_id)
            chunks = self._storage.download_stream(
                file.storage_key, self._settings.processing_read_chunk_bytes
            )
            # aclosing: if analysis stops early, close the generator (and its
            # HTTP response) now, not whenever the garbage collector gets to it.
            async with aclosing(chunks):
                stats = await analyze(chunks)
            if stats.sha256 != file.checksum_sha256:
                raise ChecksumMismatchError(f"expected {file.checksum_sha256}, got {stats.sha256}")
        except Exception as exc:
            # Every failure is final for now; Phase 08 splits retryable from permanent.
            error = f"{type(exc).__name__}: {exc}"
            log.warning("task %s: failed: %s", task_id, error)
            return await self._tasks.fail(task_id, error)

        processing_ms = round((time.perf_counter() - started) * 1000)
        log.info("task %s: completed in %d ms (%d bytes)", task_id, processing_ms, stats.size)
        return await self._tasks.complete(task_id, stats, processing_ms)
