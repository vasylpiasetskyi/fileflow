"""SQL for the `outbox_events` table. Never commits; see FileRepository."""

import uuid
from collections.abc import Sequence

from sqlalchemy import func, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.events import OutboxMessage
from app.models import OutboxEvent, OutboxStatus


class OutboxRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    def add(self, topic: str, event: OutboxMessage) -> OutboxEvent:
        row = OutboxEvent(
            id=event.event_id,
            topic=topic,
            key=event.key.decode(),
            payload=event.model_dump(mode="json"),
            status=OutboxStatus.PENDING,
            attempts=0,
        )
        self._session.add(row)
        return row

    async def claim_pending(self, limit: int) -> list[OutboxEvent]:
        """Lock up to `limit` PENDING rows, oldest first, until the transaction ends.

            SELECT ... WHERE status = 'PENDING' ORDER BY created_at
            LIMIT :limit FOR UPDATE SKIP LOCKED

        A second publisher instance skips rows the first one holds, so a row
        is not sent by two publishers at the same time.
        """
        stmt = (
            select(OutboxEvent)
            .where(OutboxEvent.status == OutboxStatus.PENDING)
            .order_by(OutboxEvent.created_at)
            .limit(limit)
            .with_for_update(skip_locked=True)
        )
        return list((await self._session.scalars(stmt)).all())

    async def mark_published(self, ids: Sequence[uuid.UUID]) -> None:
        if not ids:
            return
        await self._session.execute(
            update(OutboxEvent)
            .where(OutboxEvent.id.in_(ids))
            .values(
                status=OutboxStatus.PUBLISHED,
                attempts=OutboxEvent.attempts + 1,
                published_at=func.now(),
            )
        )

    async def record_failure(self, ids: Sequence[uuid.UUID], error: str) -> None:
        """Rows stay PENDING; the next relay round tries them again."""
        if not ids:
            return
        await self._session.execute(
            update(OutboxEvent)
            .where(OutboxEvent.id.in_(ids))
            .values(attempts=OutboxEvent.attempts + 1, last_error=error)
        )
