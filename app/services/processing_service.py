"""Processing use case: claim the task named by an event, analyze its file, record the outcome.

Idempotency. Kafka delivers at least once and the outbox may publish twice, so
the same TaskQueuedEvent can arrive again — after the task completed, or while
another worker still processes it. process() turns every such case into an
explicit, harmless Outcome instead of an error. Three mechanisms make that safe:

1. state machine: only a PENDING task can be claimed (start);
2. fencing token: only the owner of the current attempt may finish it;
3. UNIQUE(task_results.task_id): even a bug in 1-2 cannot store two results.

The natural idempotency key is task_id, so no separate "processed event ids"
(inbox) table is needed.

Failures are classified (app/retry.py): a retryable one with attempts left
moves the task to RETRY with a backoff delay (the recovery process requeues it
later); a permanent one, or the last attempt, moves it to FAILED.
"""

import enum
import logging
import time
from contextlib import aclosing
from dataclasses import dataclass

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.config import Settings
from app.events import TaskQueuedEvent
from app.models import Task, TaskStatus
from app.processing import ChecksumMismatchError, analyze
from app.repositories.file_repository import FileRepository
from app.retry import ErrorKind, backoff_delay, classify
from app.services.file_service import UnknownFileError
from app.services.task_service import TaskService, TaskTransitionConflictError
from app.storage import Storage
from app.task_state_machine import is_terminal

log = logging.getLogger(__name__)


class Outcome(enum.StrEnum):
    PROCESSED = "processed"  # we claimed the task and stored COMPLETED or FAILED
    DUPLICATE = "duplicate"  # task already finished earlier: nothing to do
    IN_PROGRESS = "in_progress"  # another worker holds the task right now
    SCHEDULED = "scheduled"  # task waits for a retry, which brings its own event
    STALE = "stale"  # we lost the task while processing; our result was discarded


@dataclass(frozen=True)
class ProcessingResult:
    outcome: Outcome
    task_status: TaskStatus
    attempt: int


class ProcessingService:
    def __init__(
        self,
        sessionmaker: async_sessionmaker[AsyncSession],
        storage: Storage,
        settings: Settings,
    ) -> None:
        self._sessionmaker = sessionmaker
        self._tasks = TaskService(sessionmaker, settings)
        self._storage = storage
        self._settings = settings

    async def process(self, event: TaskQueuedEvent) -> ProcessingResult:
        """Claim the event's task and process it, or explain why not.

        Raises UnknownTaskError if the task does not exist (an invalid
        message) and infrastructure errors (e.g. database down).
        """
        try:
            task = await self._tasks.start(event.task_id, self._settings.worker_id)
        except TaskTransitionConflictError as exc:
            if is_terminal(exc.current):
                outcome = Outcome.DUPLICATE
            elif exc.current is TaskStatus.RETRY:
                outcome = Outcome.SCHEDULED
            else:
                outcome = Outcome.IN_PROGRESS
            return ProcessingResult(outcome, exc.current, exc.current_attempt)

        # The database, not the message, is the source of truth for the file.
        try:
            finished = await self._process(task)
        except TaskTransitionConflictError as exc:
            # Our fencing token (attempt) no longer matches: someone else owns
            # the task now. Our work is thrown away, theirs stays intact.
            log.warning("task %s: lost ownership of attempt %d: %s", task.id, task.attempt, exc)
            return ProcessingResult(Outcome.STALE, exc.current, exc.current_attempt)
        return ProcessingResult(Outcome.PROCESSED, finished.status, finished.attempt)

    async def _process(self, task: Task) -> Task:
        task_id, file_id = task.id, task.file_id
        log.info("task %s: attempt %d, processing file %s", task_id, task.attempt, file_id)
        started = time.perf_counter()
        try:
            async with self._sessionmaker() as session:
                file = await FileRepository(session).get(file_id)
            if file is None:
                raise UnknownFileError(file_id)
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
            return await self._handle_failure(task, exc)

        processing_ms = round((time.perf_counter() - started) * 1000)
        log.info("task %s: completed in %d ms (%d bytes)", task_id, processing_ms, stats.size)
        return await self._tasks.complete(task_id, task.attempt, stats, processing_ms)

    async def _handle_failure(self, task: Task, exc: Exception) -> Task:
        error = f"{type(exc).__name__}: {exc}"
        kind = classify(exc)
        settings = self._settings

        if kind is ErrorKind.RETRYABLE and task.attempt < task.max_attempts:
            delay = backoff_delay(
                task.attempt,
                base_s=settings.retry_base_delay_s,
                factor=settings.retry_backoff_factor,
                max_s=settings.retry_max_delay_s,
                jitter=settings.retry_jitter,
            )
            log.warning(
                "task %s: attempt %d/%d failed (%s), retry in %.1fs: %s",
                task.id,
                task.attempt,
                task.max_attempts,
                kind,
                delay,
                error,
            )
            return await self._tasks.schedule_retry(task.id, task.attempt, error, delay)

        if kind is ErrorKind.RETRYABLE:
            error = f"gave up after {task.attempt} attempts: {error}"
        log.warning(
            "task %s: attempt %d failed (%s), giving up: %s", task.id, task.attempt, kind, error
        )
        return await self._tasks.fail(task.id, task.attempt, error)
