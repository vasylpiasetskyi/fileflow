"""Idempotent processing: duplicates, concurrent duplicates, stale workers.

The same TaskQueuedEvent can arrive more than once (Kafka redelivery, outbox
re-publish). Whatever the timing, a task must end up with exactly one result,
produced by its current owner.
"""

import asyncio
import uuid

import pytest
from fastapi import FastAPI
from sqlalchemy import func, select, text
from sqlalchemy.exc import IntegrityError

from app.config import get_settings
from app.events import TaskQueuedEvent
from app.models import TaskResult, TaskStatus
from app.processing import FileStats
from app.services import processing_service
from app.services.processing_service import Outcome, ProcessingService
from app.services.task_service import TaskService, TaskTransitionConflictError

pytestmark = pytest.mark.integration

STATS = FileStats(size=1, sha256="0" * 64, line_count=0, word_count=1)


def _service(app: FastAPI, worker_id: str) -> ProcessingService:
    settings = get_settings().model_copy(update={"worker_id": worker_id})
    return ProcessingService(app.state.sessionmaker, app.state.storage, settings)


async def _upload(client) -> TaskQueuedEvent:
    body = (await client.post("/files", files={"file": ("d.txt", b"duplicate me\n")})).json()
    return TaskQueuedEvent(task_id=body["task"]["id"], file_id=body["file"]["id"])


async def _results(app: FastAPI, task_id: uuid.UUID) -> int:
    async with app.state.sessionmaker() as session:
        return await session.scalar(
            select(func.count()).select_from(TaskResult).where(TaskResult.task_id == task_id)
        )


async def _simulate_recovery(app: FastAPI, task_id: uuid.UUID) -> None:
    """What the recovery process (Phase 11) will do with an expired lease."""
    async with app.state.sessionmaker.begin() as session:
        await session.execute(
            text("UPDATE tasks SET status = 'PENDING' WHERE id = :id"), {"id": task_id}
        )


async def test_redelivered_event_after_completion_is_a_no_op(app, client):
    event = await _upload(client)
    first = await _service(app, "w1").process(event)

    again = await _service(app, "w2").process(event)

    assert first.outcome is Outcome.PROCESSED
    assert again.outcome is Outcome.DUPLICATE
    assert again.task_status is TaskStatus.COMPLETED
    assert again.attempt == 1  # the duplicate did not start a new attempt
    assert await _results(app, event.task_id) == 1


async def test_concurrent_copies_of_one_event_produce_one_result(app, client):
    event = await _upload(client)
    workers = [_service(app, f"w{i}") for i in range(5)]

    results = await asyncio.gather(*(w.process(event) for w in workers))

    outcomes = sorted(r.outcome for r in results)
    assert outcomes.count(Outcome.PROCESSED) == 1
    assert set(outcomes) - {Outcome.PROCESSED} <= {Outcome.IN_PROGRESS, Outcome.DUPLICATE}
    assert await _results(app, event.task_id) == 1


async def test_stale_owner_cannot_finish_a_task_it_lost(app, client):
    """The fencing token, step by step with TaskService."""
    event = await _upload(client)
    tasks = TaskService(app.state.sessionmaker)

    a = await tasks.start(event.task_id, "worker-a")  # attempt 1
    await _simulate_recovery(app, event.task_id)  # A's lease "expired"
    b = await tasks.start(event.task_id, "worker-b")  # attempt 2, still PROCESSING

    # Status alone (PROCESSING) would let A through. The attempt does not.
    with pytest.raises(TaskTransitionConflictError) as exc_info:
        await tasks.complete(event.task_id, a.attempt, STATS, processing_ms=1)
    assert exc_info.value.current_attempt == 2

    done = await tasks.complete(event.task_id, b.attempt, STATS, processing_ms=1)
    assert done.worker_id == "worker-b"
    assert await _results(app, event.task_id) == 1


async def test_worker_that_loses_its_task_mid_processing_reports_stale(
    app, client, monkeypatch: pytest.MonkeyPatch
):
    """The same race through ProcessingService, in its dangerous order:
    B has claimed the task (PROCESSING, attempt 2) but not finished it when
    A, the previous owner, tries to finish. Only the attempt tells them apart."""
    event = await _upload(client)
    tasks = TaskService(app.state.sessionmaker)
    real_analyze = processing_service.analyze
    b_claims = []

    async def analyze_while_being_overtaken(chunks):
        stats = await real_analyze(chunks)
        await _simulate_recovery(app, event.task_id)  # A's lease "expired"
        b_claims.append(await tasks.start(event.task_id, "worker-b"))  # B: attempt 2
        return stats

    monkeypatch.setattr(processing_service, "analyze", analyze_while_being_overtaken)

    a = await _service(app, "worker-a").process(event)

    assert a.outcome is Outcome.STALE
    assert a.task_status is TaskStatus.PROCESSING  # still B's, untouched by A
    assert a.attempt == 2
    assert await _results(app, event.task_id) == 0  # A's result was discarded

    [b] = b_claims
    await tasks.complete(event.task_id, b.attempt, STATS, processing_ms=1)
    assert await _results(app, event.task_id) == 1


async def test_unique_task_id_is_the_last_line_of_defense(app, client):
    """Even if the guards above had a bug, PostgreSQL refuses a second result."""
    event = await _upload(client)
    tasks = TaskService(app.state.sessionmaker)
    task = await tasks.start(event.task_id, "w")
    await tasks.complete(event.task_id, task.attempt, STATS, processing_ms=1)

    with pytest.raises(IntegrityError):
        async with app.state.sessionmaker.begin() as session:
            session.add(
                TaskResult(
                    task_id=event.task_id,
                    sha256="1" * 64,
                    size=1,
                    line_count=0,
                    word_count=1,
                    processing_ms=1,
                )
            )
