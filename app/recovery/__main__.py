"""Recovery process: requeues tasks whose retry is due.

    uv run python -m app.recovery

Polls every recovery_poll_interval_s (immediately again while it finds work).
Needs only PostgreSQL: the requeued tasks' events go through the outbox.
Several instances are safe (SKIP LOCKED); one is enough.
"""

import asyncio
import contextlib
import logging
import signal

from app.config import Settings, get_settings
from app.db import create_engine, create_sessionmaker
from app.services.recovery_service import RecoveryService

log = logging.getLogger("app.recovery")


async def run(settings: Settings, stop: asyncio.Event) -> None:
    engine = create_engine(settings)
    service = RecoveryService(create_sessionmaker(engine), settings.kafka_topic)
    log.info("recovery started")
    try:
        while not stop.is_set():
            try:
                requeued = await service.requeue_due_retries(settings.recovery_batch_size)
            except Exception:
                log.exception("recovery round failed")  # e.g. PostgreSQL down
                requeued = 0
            if requeued == 0:
                with contextlib.suppress(TimeoutError):
                    await asyncio.wait_for(stop.wait(), settings.recovery_poll_interval_s)
    finally:
        log.info("recovery stopping")
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
