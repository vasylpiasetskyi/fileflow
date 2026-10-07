"""SQLAlchemy ORM models (the PostgreSQL schema). Alembic migrations are generated from these."""

import enum
import uuid
from datetime import datetime

from sqlalchemy import (
    BigInteger,
    CheckConstraint,
    DateTime,
    Enum,
    ForeignKey,
    Index,
    Integer,
    MetaData,
    String,
    Text,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

# Deterministic constraint names, so Alembic can later drop/alter them by name.
NAMING_CONVENTION = {
    "ix": "ix_%(column_0_label)s",
    "uq": "uq_%(table_name)s_%(column_0_name)s",
    "ck": "ck_%(table_name)s_%(constraint_name)s",
    "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
    "pk": "pk_%(table_name)s",
}


class Base(DeclarativeBase):
    metadata = MetaData(naming_convention=NAMING_CONVENTION)


def status_column(enum_cls: type[enum.StrEnum]) -> Enum:
    """VARCHAR column mapped to a Python enum, *without* an implicit CHECK.

    VARCHAR instead of a native PG enum: adding a status later is a plain
    constraint change, not an ALTER TYPE. The CHECK is declared explicitly via
    `status_check()`; letting Enum create it as well makes Alembic autogenerate
    emit two identical constraints.
    """
    return Enum(enum_cls, native_enum=False, create_constraint=False, length=32)


def status_check(enum_cls: type[enum.StrEnum]) -> CheckConstraint:
    allowed = ", ".join(f"'{member.value}'" for member in enum_cls)
    return CheckConstraint(f"status IN ({allowed})", name=enum_cls.__name__.lower())


class FileStatus(enum.StrEnum):
    UPLOADING = "UPLOADING"  # row exists, bytes are (maybe) still streaming to S3
    UPLOADED = "UPLOADED"  # object complete in S3, size/checksum known
    FAILED = "FAILED"  # storage rejected the upload; no object was created


class TaskStatus(enum.StrEnum):
    PENDING = "PENDING"  # waiting for a worker
    PROCESSING = "PROCESSING"  # claimed by a worker
    RETRY = "RETRY"  # a retryable failure; waits until next_attempt_at
    COMPLETED = "COMPLETED"  # terminal: processed successfully
    FAILED = "FAILED"  # terminal: processing gave up


class OutboxStatus(enum.StrEnum):
    PENDING = "PENDING"  # committed, not yet confirmed by Kafka
    PUBLISHED = "PUBLISHED"  # Kafka acked it (it may still have been sent more than once)


class File(Base):
    __tablename__ = "files"
    __table_args__ = (status_check(FileStatus),)

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True)
    filename: Mapped[str] = mapped_column(String(255))
    content_type: Mapped[str] = mapped_column(String(255))
    # Unknown until the stream has been fully consumed.
    size: Mapped[int | None] = mapped_column(BigInteger)
    checksum_sha256: Mapped[str | None] = mapped_column(String(64))
    storage_key: Mapped[str] = mapped_column(String(512), unique=True)
    status: Mapped[FileStatus] = mapped_column(status_column(FileStatus), index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )


class Task(Base):
    """One processing job for one file. Allowed status changes: app/task_state_machine.py."""

    __tablename__ = "tasks"
    # Fetch server-generated columns (created_at, ...) via INSERT ... RETURNING,
    # so the object is complete right after commit without an extra SELECT.
    __mapper_args__ = {"eager_defaults": True}
    __table_args__ = (
        status_check(TaskStatus),
        # The recovery process looks only for RETRY tasks whose time has come.
        Index(
            "ix_tasks_retry_next_attempt_at",
            "next_attempt_at",
            postgresql_where=text("status = 'RETRY'"),
        ),
    )

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True)
    file_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("files.id"), index=True)
    status: Mapped[TaskStatus] = mapped_column(status_column(TaskStatus), index=True)
    # Incremented on every PENDING -> PROCESSING; also the fencing token.
    attempt: Mapped[int] = mapped_column(Integer, server_default="0")
    # The most recent failure (kept while retrying, cleared on success).
    error: Mapped[str | None] = mapped_column(Text)
    # When a RETRY task becomes due again (database clock).
    next_attempt_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    # The worker that claimed the task most recently.
    worker_id: Mapped[str | None] = mapped_column(String(255))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class TaskResult(Base):
    """Output of a completed task.

    task_id is the primary key: at most one result per task, enforced by
    PostgreSQL. Written in the same transaction as PROCESSING -> COMPLETED.
    """

    __tablename__ = "task_results"
    __mapper_args__ = {"eager_defaults": True}

    task_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("tasks.id"), primary_key=True)
    sha256: Mapped[str] = mapped_column(String(64))
    size: Mapped[int] = mapped_column(BigInteger)
    line_count: Mapped[int] = mapped_column(BigInteger)
    word_count: Mapped[int] = mapped_column(BigInteger)
    processing_ms: Mapped[int] = mapped_column(Integer)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


class OutboxEvent(Base):
    """A message waiting to be relayed to Kafka (transactional outbox).

    Written in the same transaction as the state change it announces; the
    outbox publisher process sends it later. The row id is the event_id inside
    the payload, so every copy of a message in Kafka carries the same id.
    """

    __tablename__ = "outbox_events"
    __mapper_args__ = {"eager_defaults": True}
    __table_args__ = (
        status_check(OutboxStatus),
        # The publisher only ever looks for PENDING rows, oldest first. A
        # partial index stays small while PUBLISHED rows pile up.
        Index(
            "ix_outbox_events_pending_created_at",
            "created_at",
            postgresql_where=text("status = 'PENDING'"),
        ),
    )

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True)
    topic: Mapped[str] = mapped_column(String(255))
    key: Mapped[str] = mapped_column(String(255))
    payload: Mapped[dict] = mapped_column(JSONB)
    status: Mapped[OutboxStatus] = mapped_column(status_column(OutboxStatus))
    # Publish attempts (successful or not) and the last failure, for visibility.
    attempts: Mapped[int] = mapped_column(Integer, server_default="0")
    last_error: Mapped[str | None] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    published_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
