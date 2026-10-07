"""Outbox publisher process: relays outbox_events rows to Kafka.

    uv run python -m app.outbox

Runs rounds back to back while there is work; when the outbox is empty or a
round failed, pauses for outbox_poll_interval_s. Stops on SIGTERM/SIGINT
between rounds. Several instances may run (SKIP LOCKED keeps them apart), but
one is enough — and with one, events leave in commit order.
"""

import asyncio
import contextlib
import logging
import signal

from app.config import Settings, get_settings
from app.db import create_engine, create_sessionmaker
from app.kafka import EventPublisher, require_topics
from app.services.outbox_relay import OutboxRelay

log = logging.getLogger("app.outbox")


async def run(settings: Settings, stop: asyncio.Event) -> None:
    engine = create_engine(settings)
    publisher = EventPublisher(settings)
    try:
        # Exits if Kafka is unreachable at startup; the supervisor restarts us.
        # Once running, Kafka outages are survived: rows simply stay PENDING.
        # The outbox carries both task events and dead letters.
        await require_topics(settings, settings.kafka_topic, settings.kafka_dlq_topic)
        await publisher.start()
        relay = OutboxRelay(create_sessionmaker(engine), publisher, settings)
        log.info("outbox publisher started")
        while not stop.is_set():
            try:
                published, failed = await relay.relay_once()
            except Exception:
                # e.g. PostgreSQL down: nothing was marked, nothing is lost.
                log.exception("outbox round failed")
                published, failed = 0, 1
            if published == 0 or failed:
                with contextlib.suppress(TimeoutError):
                    await asyncio.wait_for(stop.wait(), settings.outbox_poll_interval_s)
    finally:
        log.info("outbox publisher stopping")
        await publisher.stop()
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
