"""Worker process: a Kafka consumer that processes TaskQueuedEvents.

    uv run python -m app.worker

Delivery semantics: at-least-once. Auto-commit is off; the offset of a message
is committed only after its outcome is stored in PostgreSQL. A crash between
the two means the message is delivered again after restart/rebalance.

Per message:
- processed, or a harmless duplicate (see ProcessingService.process) -> commit
- invalid message or unknown task -> dead letter to the DLQ topic, THEN commit
- infrastructure error (e.g. DB down, DLQ unreachable) -> no commit, seek back, retry

Dead letters for rejected messages are published directly (not via the outbox):
what must not be lost here is the Kafka message itself, and it is "kept" for
as long as its offset is not committed. Publishing to the DLQ and only then
committing the offset gives at-least-once delivery into the DLQ. (Failed TASKS
are a database fact; their dead letters go through the outbox — see TaskService.fail.)

Stops on SIGTERM/SIGINT *between* messages (Phase 12 refines shutdown).
"""

import asyncio
import contextlib
import logging
import signal
from datetime import UTC, datetime

from aiokafka import AIOKafkaConsumer, ConsumerRecord, TopicPartition
from pydantic import ValidationError

from app.config import Settings, get_settings
from app.db import create_engine, create_sessionmaker
from app.events import DeadLetter, DeadLetterKind, TaskQueuedEvent
from app.failure_lab import crash_point
from app.kafka import EventPublisher, PublishError, require_topics
from app.services.processing_service import Outcome, ProcessingService
from app.services.task_service import UnknownTaskError
from app.storage import Storage

log = logging.getLogger("app.worker")


def create_consumer(settings: Settings) -> AIOKafkaConsumer:
    return AIOKafkaConsumer(
        settings.kafka_topic,
        bootstrap_servers=settings.kafka_bootstrap_servers,
        group_id=settings.kafka_consumer_group,
        client_id=settings.worker_id,
        # We commit explicitly after processing; auto-commit would commit on a
        # timer, possibly before the work is done (-> lost messages on crash).
        enable_auto_commit=False,
        # A group with no committed offset yet starts from the oldest message,
        # so events published before the first worker started are not lost.
        auto_offset_reset="earliest",
    )


async def handle(
    consumer: AIOKafkaConsumer,
    service: ProcessingService,
    publisher: EventPublisher,
    msg: ConsumerRecord,
    settings: Settings,
    stop: asyncio.Event,
) -> None:
    tp = TopicPartition(msg.topic, msg.partition)
    where = f"{msg.topic}[{msg.partition}]@{msg.offset}"

    async def rewind() -> None:
        # The message's outcome was NOT secured: no commit, fetch it again later.
        consumer.seek(tp, msg.offset)
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(stop.wait(), settings.worker_redelivery_delay_s)

    async def dead_letter(kind: DeadLetterKind, error: str, **fields: object) -> bool:
        letter = DeadLetter(
            kind=kind,
            error=error,
            failed_at=datetime.now(UTC),
            source=where,
            raw_value=(msg.value or b"").decode("utf-8", errors="replace"),
            **fields,
        )
        try:
            await publisher.publish(
                settings.kafka_dlq_topic, letter.key, letter.model_dump_json().encode()
            )
        except PublishError:
            log.exception("%s: cannot dead-letter the message, will redeliver", where)
            await rewind()
            return False
        log.error("%s: dead-lettered (%s): %s", where, kind, error)
        return True

    try:
        event = TaskQueuedEvent.model_validate_json(msg.value)
    except ValidationError as exc:
        # A poison message: retrying cannot fix it.
        if not await dead_letter(DeadLetterKind.INVALID_MESSAGE, str(exc.errors()[0]["msg"])):
            return
    else:
        try:
            result = await service.process(event)
        except UnknownTaskError:
            if not await dead_letter(
                DeadLetterKind.UNKNOWN_TASK,
                f"task {event.task_id} does not exist",
                task_id=event.task_id,
                file_id=event.file_id,
                attempt=event.attempt,
            ):
                return
        except Exception:
            log.exception("%s: handling failed, will redeliver", where)
            await rewind()
            return
        else:
            if result.outcome is Outcome.PROCESSED:
                log.info("%s: task %s -> %s", where, event.task_id, result.task_status)
            else:
                # Expected under at-least-once delivery: not an error.
                log.info(
                    "%s: event %s is a no-op (%s): task %s is %s, attempt %d",
                    where,
                    event.event_id,
                    result.outcome,
                    event.task_id,
                    result.task_status,
                    result.attempt,
                )

    crash_point(settings.worker_crash_point, "after_processing")
    # The committed offset is the *next* message to read, hence + 1.
    await consumer.commit({tp: msg.offset + 1})


async def run(settings: Settings, stop: asyncio.Event) -> None:
    engine = create_engine(settings)
    storage = Storage(settings)
    await storage.start()
    service = ProcessingService(create_sessionmaker(engine), storage, settings)
    consumer = create_consumer(settings)
    publisher = EventPublisher(settings)  # for dead letters of rejected messages
    try:
        # Raises if Kafka is unreachable or a topic is missing — the process
        # exits and its supervisor (compose `restart:`, Kubernetes) starts it again.
        await require_topics(settings, settings.kafka_topic, settings.kafka_dlq_topic)
        await publisher.start()
        await consumer.start()  # joins the consumer group; partitions get assigned
        log.info("worker %s consuming %s", settings.worker_id, settings.kafka_topic)
        while not stop.is_set():
            # Short timeout so a stop request is noticed within a second even
            # when the topic is idle; one record at a time keeps offsets simple.
            batch = await consumer.getmany(timeout_ms=1000, max_records=1)
            for messages in batch.values():
                for msg in messages:
                    await handle(consumer, service, publisher, msg, settings, stop)
    finally:
        log.info("worker %s stopping", settings.worker_id)
        # stop() sends LeaveGroup: partitions are reassigned immediately instead
        # of after the session timeout (which is what a kill -9 costs).
        await consumer.stop()
        await publisher.stop()
        await storage.close()
        await engine.dispose()


async def main_async() -> None:
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, stop.set)
    await run(get_settings(), stop)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    asyncio.run(main_async())


if __name__ == "__main__":
    main()
