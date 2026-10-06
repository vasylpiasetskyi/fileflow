"""Phase 03 experiment: N workers race to claim the same PENDING task.

    uv run python -m experiments.phase03_claim_race

naive:   SELECT status -> check in Python -> UPDATE      (read-check-write)
guarded: UPDATE ... WHERE status = 'PENDING' RETURNING  (one atomic statement)

The sleep between read and write stands in for any real gap (network hop,
GC pause, a busy event loop). Requires `docker compose up` and migrations.
"""

import asyncio
import uuid

from sqlalchemy import text

from app.config import get_settings
from app.db import create_engine

WORKERS = 10


async def create_pending_task(engine) -> uuid.UUID:
    file_id, task_id = uuid.uuid4(), uuid.uuid4()
    async with engine.begin() as conn:
        await conn.execute(
            text(
                "INSERT INTO files (id, filename, content_type, storage_key, status)"
                " VALUES (:id, 'race.txt', 'text/plain', :key, 'UPLOADED')"
            ),
            {"id": file_id, "key": f"experiments/{file_id}"},
        )
        await conn.execute(
            text("INSERT INTO tasks (id, file_id, status) VALUES (:id, :file_id, 'PENDING')"),
            {"id": task_id, "file_id": file_id},
        )
    return task_id


async def naive_claim(engine, task_id: uuid.UUID) -> bool:
    async with engine.begin() as conn:
        status = await conn.scalar(text("SELECT status FROM tasks WHERE id = :id"), {"id": task_id})
        if status != "PENDING":
            return False
        await asyncio.sleep(0.05)  # the gap between "check" and "act"
        await conn.execute(
            text("UPDATE tasks SET status = 'PROCESSING', attempt = attempt + 1 WHERE id = :id"),
            {"id": task_id},
        )
        return True


async def guarded_claim(engine, task_id: uuid.UUID) -> bool:
    async with engine.begin() as conn:
        claimed = await conn.scalar(
            text(
                "UPDATE tasks SET status = 'PROCESSING', attempt = attempt + 1"
                " WHERE id = :id AND status = 'PENDING' RETURNING id"
            ),
            {"id": task_id},
        )
        await asyncio.sleep(0.05)  # same delay, but now *after* the atomic decision
        return claimed is not None


async def run(engine, name: str, claim) -> None:
    task_id = await create_pending_task(engine)
    results = await asyncio.gather(*(claim(engine, task_id) for _ in range(WORKERS)))
    async with engine.connect() as conn:
        attempt = await conn.scalar(
            text("SELECT attempt FROM tasks WHERE id = :id"), {"id": task_id}
        )
    print(f"{name:8} workers that think they own the task: {sum(results):2}   attempt = {attempt}")


async def main() -> None:
    settings = get_settings()
    settings.db_pool_size = WORKERS  # one connection per worker, no queueing in the pool
    engine = create_engine(settings)
    try:
        await run(engine, "naive", naive_claim)
        await run(engine, "guarded", guarded_claim)
    finally:
        await engine.dispose()


if __name__ == "__main__":
    asyncio.run(main())
