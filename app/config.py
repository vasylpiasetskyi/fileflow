"""Application configuration.

All settings come from environment variables (or a local `.env` file).
Defaults target the *hybrid* dev setup: infrastructure in docker compose,
Python processes on the host, so every host is `localhost`. Inside compose
the app containers override these with service names (`postgres`, `minio`, `kafka`).
"""

import os
import socket
from functools import lru_cache
from typing import Literal

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="FILEFLOW_", env_file=".env", extra="ignore")

    # PostgreSQL
    database_url: str = "postgresql+asyncpg://fileflow:fileflow@localhost:5432/fileflow"
    db_pool_size: int = 5

    # MinIO / S3
    s3_endpoint_url: str = "http://localhost:9000"
    s3_access_key: str = "fileflow"
    s3_secret_key: str = "fileflow-secret"
    s3_bucket: str = "fileflow"
    s3_region: str = "us-east-1"
    # S3 multipart upload part size. S3 requires >= 5 MiB for every part except
    # the last one. This is also the per-upload memory bound: one part is
    # buffered at a time.
    s3_part_size_bytes: int = Field(default=8 * 1024 * 1024, ge=5 * 1024 * 1024)

    # How much we read from the incoming upload per iteration.
    upload_read_chunk_bytes: int = 1024 * 1024
    # Largest accepted upload. Enforced while streaming (and up front when the
    # client declares Content-Length), so an oversized body is cut off early.
    max_upload_bytes: int = 10 * 1024**3

    # Kafka
    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_topic: str = "file-processing"
    # Dead letters: tasks that failed for good and messages that cannot be processed.
    kafka_dlq_topic: str = "file-processing-dlq"
    # All workers share one consumer group: Kafka splits the topic's
    # partitions between them, each partition goes to exactly one member.
    kafka_consumer_group: str = "fileflow-workers"
    # Per-request timeout of the Kafka client (metadata, produce requests).
    kafka_request_timeout_ms: int = 5000
    # Upper bound for one publish() call. Needed because an idempotent producer
    # retries a failing send indefinitely: without it, a relay round hangs (with
    # its outbox rows locked) for as long as Kafka is down.
    kafka_publish_timeout_s: float = 10.0

    # Outbox publisher
    # Pause between relay rounds when the outbox is empty or Kafka failed.
    outbox_poll_interval_s: float = 0.5
    # Max rows locked and sent per round (one transaction).
    outbox_batch_size: int = 100
    # FAILURE LAB ONLY. Kill the process (os._exit) at this point of a round:
    # "before_publish" (rows locked, nothing sent) or "after_publish" (Kafka
    # acked, rows not yet marked PUBLISHED). Empty = never.
    outbox_crash_point: Literal["", "before_publish", "after_publish"] = ""

    # Worker
    # Identifies which process claimed a task. hostname = container id under compose.
    worker_id: str = Field(default_factory=lambda: f"{socket.gethostname()}-{os.getpid()}")
    # Pause before re-reading a message whose handling hit an infrastructure
    # error (e.g. PostgreSQL down). Proper retries with backoff: Phase 08.
    worker_redelivery_delay_s: float = 2.0
    # How much of an object the worker reads from storage per iteration.
    processing_read_chunk_bytes: int = 1024 * 1024
    # FAILURE LAB ONLY. "after_processing": kill the worker after the task
    # outcome is committed to PostgreSQL but before the Kafka offset is
    # committed -> the message is redelivered. Empty = never.
    worker_crash_point: Literal["", "after_processing"] = ""

    # Retries (see app/retry.py)
    # Attempt budget given to a new task (and added again by a DLQ redrive);
    # stored per task in tasks.max_attempts. Then FAILED.
    task_max_attempts: int = Field(default=5, ge=1)
    # Delay before retry n (n = attempt that failed):
    #   min(max, base * factor^(n-1)) * random(1 - jitter, 1 + jitter)
    # Defaults: ~5s, ~25s, ~2min, ~10min.
    retry_base_delay_s: float = 5.0
    retry_backoff_factor: float = 5.0
    retry_max_delay_s: float = 900.0
    retry_jitter: float = Field(default=0.2, ge=0, lt=1)

    # Recovery process: requeues RETRY tasks once they are due.
    recovery_poll_interval_s: float = 1.0
    recovery_batch_size: int = 100

    # Health checks: how long a single dependency probe may take
    health_check_timeout_s: float = 2.0


@lru_cache
def get_settings() -> Settings:
    return Settings()
