"""Operator tool for the dead letter queue.

    uv run python -m app.dlq list [--limit N]   # what is in the DLQ
    uv run python -m app.dlq redrive TASK_ID    # give a FAILED task another chance

`list` reads the DLQ topic from the beginning without a consumer group, so it
commits nothing and can be run any number of times. The DLQ is a log, not a
queue: redriving a task does not remove its dead letter.

`redrive` is for after the cause was fixed (dependency back, bug deployed):
FAILED -> PENDING with a new attempt budget and a new event via the outbox.
"""

import argparse
import asyncio
import sys
import uuid

from aiokafka import AIOKafkaConsumer
from pydantic import ValidationError

from app.config import Settings, get_settings
from app.db import create_engine, create_sessionmaker
from app.events import DeadLetter
from app.services.task_service import TaskService, TaskTransitionConflictError, UnknownTaskError


async def list_dead_letters(settings: Settings, limit: int) -> list[DeadLetter]:
    consumer = AIOKafkaConsumer(
        settings.kafka_dlq_topic,
        bootstrap_servers=settings.kafka_bootstrap_servers,
        group_id=None,  # no group: nothing is committed, the DLQ stays as it is
        auto_offset_reset="earliest",
        enable_auto_commit=False,
    )
    await consumer.start()
    letters: list[DeadLetter] = []
    try:
        while True:
            batch = await consumer.getmany(timeout_ms=2000)
            if not batch:
                break  # caught up with the end of the topic
            for messages in batch.values():
                for msg in messages:
                    try:
                        letters.append(DeadLetter.model_validate_json(msg.value))
                    except ValidationError:
                        print(f"! unreadable dead letter at offset {msg.offset}", file=sys.stderr)
    finally:
        await consumer.stop()
    return letters[-limit:]


async def redrive(settings: Settings, task_id: uuid.UUID) -> int:
    engine = create_engine(settings)
    try:
        task = await TaskService(create_sessionmaker(engine), settings).redrive(task_id)
    except UnknownTaskError:
        print(f"task {task_id} does not exist", file=sys.stderr)
        return 1
    except TaskTransitionConflictError as exc:
        print(f"cannot redrive: task is {exc.current}, only FAILED tasks can be", file=sys.stderr)
        return 1
    finally:
        await engine.dispose()
    print(
        f"task {task.id}: FAILED -> PENDING, next attempt {task.attempt + 1}, "
        f"budget up to attempt {task.max_attempts}"
    )
    return 0


def main() -> None:
    parser = argparse.ArgumentParser(prog="python -m app.dlq", description=__doc__.split("\n")[0])
    commands = parser.add_subparsers(dest="command", required=True)
    list_cmd = commands.add_parser("list", help="show the most recent dead letters")
    list_cmd.add_argument("--limit", type=int, default=20)
    redrive_cmd = commands.add_parser("redrive", help="send a FAILED task back for processing")
    redrive_cmd.add_argument("task_id", type=uuid.UUID)
    args = parser.parse_args()
    settings = get_settings()

    if args.command == "list":
        letters = asyncio.run(list_dead_letters(settings, args.limit))
        for d in letters:
            print(
                f"{d.failed_at:%Y-%m-%d %H:%M:%S}  {d.kind:<15}  task={d.task_id or '-'}  "
                f"attempt={d.attempt if d.attempt is not None else '-'}  {d.error[:90]}"
            )
        print(f"({len(letters)} shown)")
    else:
        sys.exit(asyncio.run(redrive(settings, args.task_id)))


if __name__ == "__main__":
    main()
