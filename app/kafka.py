"""Kafka helpers. Producers/consumers arrive in Phase 05; for now only a probe."""

from aiokafka.admin import AIOKafkaAdminClient

from app.config import Settings


async def ping(settings: Settings) -> None:
    # A short-lived admin client: connect, fetch cluster metadata, disconnect.
    # Fine for a health probe; real producers/consumers will be long-lived.
    admin = AIOKafkaAdminClient(
        bootstrap_servers=settings.kafka_bootstrap_servers,
        request_timeout_ms=int(settings.health_check_timeout_s * 1000),
    )
    await admin.start()
    try:
        await admin.describe_cluster()
    finally:
        await admin.close()
