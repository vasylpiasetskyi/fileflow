"""SQLAlchemy ORM models (the PostgreSQL schema). Alembic migrations are generated from these."""

import enum
import uuid
from datetime import datetime

from sqlalchemy import BigInteger, DateTime, Enum, MetaData, String, func
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


class FileStatus(enum.StrEnum):
    UPLOADING = "UPLOADING"  # row exists, bytes are (maybe) still streaming to S3
    UPLOADED = "UPLOADED"  # object complete in S3, size/checksum known
    FAILED = "FAILED"  # storage rejected the upload; no object was created


class File(Base):
    __tablename__ = "files"

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True)
    filename: Mapped[str] = mapped_column(String(255))
    content_type: Mapped[str] = mapped_column(String(255))
    # Unknown until the stream has been fully consumed.
    size: Mapped[int | None] = mapped_column(BigInteger)
    checksum_sha256: Mapped[str | None] = mapped_column(String(64))
    storage_key: Mapped[str] = mapped_column(String(512), unique=True)
    # VARCHAR + CHECK instead of a native PG enum: adding a status later is a
    # plain constraint change, not an ALTER TYPE.
    status: Mapped[FileStatus] = mapped_column(
        Enum(FileStatus, native_enum=False, create_constraint=True, length=32),
        index=True,
    )
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )
