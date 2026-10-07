"""Relays outbox rows to Kafka: the second half of the transactional outbox.

One round = one PostgreSQL transaction:

    BEGIN
      SELECT ... FOR UPDATE SKIP LOCKED     -- claim a batch of PENDING rows
      send each row to Kafka, wait for acks
      UPDATE ... SET status = 'PUBLISHED'   -- only rows Kafka acked
    COMMIT

Failure analysis (the point of this phase):
- Kafka down          -> sends fail, rows stay PENDING (+ attempts, last_error),
                         retried next round. Nothing is lost.
- crash before send   -> transaction aborts, locks released, rows still PENDING.
- crash after send,   -> Kafka has the message, but the UPDATE never committed:
  before COMMIT          the row is still PENDING and is sent AGAIN next round.
                         A duplicate, with the same event_id. Expected: the
                         outbox gives at-least-once publishing, consumers must
                         tolerate duplicates.
"""

import asyncio
import json
import logging

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.config import Settings
from app.failure_lab import crash_point
from app.kafka import EventPublisher
from app.repositories.outbox_repository import OutboxRepository

log = logging.getLogger(__name__)


class OutboxRelay:
    def __init__(
        self,
        sessionmaker: async_sessionmaker[AsyncSession],
        publisher: EventPublisher,
        settings: Settings,
    ) -> None:
        self._sessionmaker = sessionmaker
        self._publisher = publisher
        self._settings = settings

    async def relay_once(self) -> tuple[int, int]:
        """Run one round. Returns (published, failed) row counts."""
        async with self._sessionmaker.begin() as session:
            repo = OutboxRepository(session)
            rows = await repo.claim_pending(self._settings.outbox_batch_size)
            if not rows:
                return 0, 0
            crash_point(self._settings.outbox_crash_point, "before_publish")

            # Send the whole batch concurrently: the producer groups messages
            # into one request per partition instead of one round trip each.
            results = await asyncio.gather(
                *(
                    self._publisher.publish(
                        row.topic, row.key.encode(), json.dumps(row.payload).encode()
                    )
                    for row in rows
                ),
                return_exceptions=True,
            )
            published = [
                row.id
                for row, r in zip(rows, results, strict=True)
                if not isinstance(r, BaseException)
            ]
            errors = [
                (row.id, r)
                for row, r in zip(rows, results, strict=True)
                if isinstance(r, BaseException)
            ]
            crash_point(self._settings.outbox_crash_point, "after_publish")

            await repo.mark_published(published)
            if errors:
                first = errors[0][1]
                await repo.record_failure(
                    [row_id for row_id, _ in errors], f"{type(first).__name__}: {first}"
                )

        if published:
            log.info("published %d outbox event(s)", len(published))
        if errors:
            log.warning(
                "%d outbox event(s) not published, will retry: %s", len(errors), errors[0][1]
            )
        return len(published), len(errors)
