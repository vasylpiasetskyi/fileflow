"""Worker process: polls PostgreSQL for PENDING tasks and processes them.

    uv run python -m app.worker

Stops on SIGTERM/SIGINT *between* tasks: the task in progress is finished
first. (Phase 12 refines shutdown; until leases exist, a hard kill leaves the
current task stuck in PROCESSING.)
"""

import asyncio
import contextlib
import logging
import signal

from app.config import Settings, get_settings
from app.db import create_engine, create_sessionmaker
from app.services.processing_service import ProcessingService
from app.storage import Storage

log = logging.getLogger("app.worker")


async def run(settings: Settings) -> None:
    engine = create_engine(settings)
    storage = Storage(settings)
    await storage.start()
    service = ProcessingService(create_sessionmaker(engine), storage, settings)

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, stop.set)

    log.info("worker %s started", settings.worker_id)
    try:
        while not stop.is_set():
            try:
                task = await service.process_next()
            except Exception:
                # e.g. PostgreSQL down while claiming or recording a result.
                # Keep the process alive and try again after the poll interval.
                log.exception("worker iteration failed")
                task = None
            if task is None:
                # Idle: sleep for the poll interval, but wake up at once on stop.
                with contextlib.suppress(TimeoutError):
                    await asyncio.wait_for(stop.wait(), settings.worker_poll_interval_s)
    finally:
        log.info("worker %s stopping", settings.worker_id)
        await storage.close()
        await engine.dispose()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    asyncio.run(run(get_settings()))


if __name__ == "__main__":
    main()
