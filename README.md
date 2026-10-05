# FileFlow

Asynchronous file processing lab: FastAPI, PostgreSQL, MinIO, Kafka.
A learning project for distributed-systems patterns (transactional outbox,
idempotent consumers, retries/DLQ, leases, graceful shutdown).
The phase plan is in [docs/PLAN.md](docs/PLAN.md).

> Status: Phase 01 — foundation.

## Run (hybrid dev)

Infrastructure runs in Docker, Python runs on the host:

```bash
docker compose up -d --wait          # postgres, minio (+bucket), kafka
uv sync
uv run uvicorn app.api.main:app --reload
open http://localhost:8000           # UI
curl localhost:8000/health
```

Everything in containers instead:

```bash
docker compose --profile app up -d --build
```

| Service      | Host address                                     | In-network address |
|--------------|--------------------------------------------------|--------------------|
| API          | http://localhost:8000                            | `api:8000`         |
| PostgreSQL   | localhost:5432 (fileflow/fileflow)               | `postgres:5432`    |
| MinIO S3     | http://localhost:9000                            | `minio:9000`       |
| MinIO console| http://localhost:9001 (fileflow/fileflow-secret) | —                  |
| Kafka        | localhost:9092                                   | `kafka:19092`      |

## Tests

```bash
uv run pytest                    # unit tests (no infrastructure needed)
uv run pytest -m integration     # against running compose infrastructure
```

## Layout

```text
app/
  config.py        settings from FILEFLOW_* env vars
  db.py            async SQLAlchemy engine
  storage.py       S3/MinIO client
  kafka.py         Kafka helpers
  api/
    main.py        app factory + lifespan (resource setup/teardown)
    deps.py        dependencies: app.state -> endpoints
    routes/        health, ui
templates/ static/ Jinja2 + HTMX UI
migrations/        Alembic (from Phase 02)
```
