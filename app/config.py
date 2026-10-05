"""Application configuration.

All settings come from environment variables (or a local `.env` file).
Defaults target the *hybrid* dev setup: infrastructure in docker compose,
Python processes on the host, so every host is `localhost`. Inside compose
the app containers override these with service names (`postgres`, `minio`, `kafka`).
"""

from functools import lru_cache

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

    # Kafka
    kafka_bootstrap_servers: str = "localhost:9092"

    # Health checks: how long a single dependency probe may take
    health_check_timeout_s: float = 2.0


@lru_cache
def get_settings() -> Settings:
    return Settings()
