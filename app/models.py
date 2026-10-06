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
    Integer,
    MetaData,
    String,
    Text,
    func,
)
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
    COMPLETED = "COMPLETED"  # terminal: processed successfully
    FAILED = "FAILED"  # terminal: processing gave up


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
    __table_args__ = (status_check(TaskStatus),)

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True)
    file_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("files.id"), index=True)
    status: Mapped[TaskStatus] = mapped_column(status_column(TaskStatus), index=True)
    # Incremented on every PENDING -> PROCESSING; retries (Phase 08) build on it.
    attempt: Mapped[int] = mapped_column(Integer, server_default="0")
    error: Mapped[str | None] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
