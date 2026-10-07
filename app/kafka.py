"""Kafka client wiring: health probe and event publisher."""

import asyncio

from aiokafka import AIOKafkaProducer
from aiokafka.admin import AIOKafkaAdminClient
from aiokafka.errors import KafkaError
from aiokafka.structs import RecordMetadata

from app.config import Settings


class PublishError(Exception):
    """The event could not be delivered to Kafka."""


class MissingTopicError(RuntimeError):
    """The configured topic does not exist (auto-creation is disabled)."""


async def ping(settings: Settings) -> None:
    # A short-lived admin client: connect, fetch cluster metadata, disconnect.
    # Fine for a health probe; producers/consumers are long-lived.
    admin = AIOKafkaAdminClient(
        bootstrap_servers=settings.kafka_bootstrap_servers,
        request_timeout_ms=int(settings.health_check_timeout_s * 1000),
    )
    await admin.start()
    try:
        await admin.describe_cluster()
    finally:
        await admin.close()


async def require_topic(settings: Settings) -> None:
    """Fail fast if settings.kafka_topic does not exist.

    Without this check a typo in the topic name fails silently: a consumer
    subscribed to a missing topic just waits forever, and a producer only
    times out per message.
    """
    admin = AIOKafkaAdminClient(
        bootstrap_servers=settings.kafka_bootstrap_servers,
        request_timeout_ms=settings.kafka_request_timeout_ms,
    )
    await admin.start()
    try:
        topics = await admin.list_topics()
    finally:
        await admin.close()
    if settings.kafka_topic not in topics:
        raise MissingTopicError(
            f"Kafka topic {settings.kafka_topic!r} does not exist and auto-creation is "
            "disabled; create it (see the kafka-init service in docker-compose.yml)"
        )


class EventPublisher:
    """Long-lived Kafka producer, one per process."""

    def __init__(self, settings: Settings) -> None:
        self._publish_timeout_s = settings.kafka_publish_timeout_s
        self._producer = AIOKafkaProducer(
            bootstrap_servers=settings.kafka_bootstrap_servers,
            # acks=all: the broker confirms only after the write is replicated
            # to all in-sync replicas (one, in our single-broker setup).
            acks="all",
            # Producer idempotence: if the producer retries a send whose ack got
            # lost, the broker drops the duplicate. This protects one send call;
            # it does NOT make consumers idempotent (Phase 07).
            enable_idempotence=True,
            request_timeout_ms=settings.kafka_request_timeout_ms,
        )

    async def start(self) -> None:
        # Connects to the cluster: fails if Kafka is down at startup.
        await self._producer.start()

    async def stop(self) -> None:
        # Flushes buffered messages, then closes connections.
        await self._producer.stop()

    async def publish(self, topic: str, key: bytes, value: bytes) -> RecordMetadata:
        """Send and wait for the broker's ack, at most kafka_publish_timeout_s.

        A timeout does NOT mean "not sent": the message already sits in the
        producer's buffer and may still reach Kafka later. The outcome is
        unknown — which is fine for the outbox: the row stays PENDING and is
        sent again, at worst producing a duplicate.
        """
        try:
            async with asyncio.timeout(self._publish_timeout_s):
                return await self._producer.send_and_wait(topic, value=value, key=key)
        except (KafkaError, TimeoutError) as exc:
            raise PublishError(f"cannot publish to {topic}: {exc!r}") from exc
