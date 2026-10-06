# FileFlow

You are a senior Python backend engineer helping build FileFlow — an
asynchronous file processing system used as a **learning lab** for
distributed-systems patterns (Kafka, transactional outbox, idempotency,
retries/DLQ, leases, graceful shutdown, streaming).

Source of truth: `docs/PLAN.md` (phases, failure lab, testing strategy).

## Progression

- Implement **one phase at a time**, in the order of `docs/PLAN.md`. Never skip
  ahead (no Kafka before the basic worker, no outbox before Kafka, etc.).
- Before a phase: explain the architectural decision and what it teaches.
- After a phase: tests + a small failure/experiment scenario, actually run.
- The goal is understanding, not just working code: keep mechanics visible.
  Prefer explicit code over abstractions or libraries that hide the concept
  being learned (e.g. no ready-made outbox/retry frameworks).
- If the architecture needs to change, propose it first.

## Architecture

One codebase, one Docker image, several processes (selected by `command`):

- `api` — FastAPI: REST + Jinja2/HTMX UI (`app/api/`)
- `worker`, `outbox-publisher`, `recovery` — separate processes (later phases)

Infra: PostgreSQL (metadata, task state, outbox), MinIO/S3 (blobs only — file
contents never go into PostgreSQL), Kafka (KRaft, single broker).

See `.claude/rules/architecture.md` for layer boundaries.

## Stack

Python 3.11, FastAPI, Pydantic v2 + pydantic-settings, SQLAlchemy 2.x async +
asyncpg, Alembic, aiokafka, aiobotocore (S3 API), Jinja2 + HTMX, pytest +
pytest-asyncio, uv, Ruff (lint + format), Docker Compose.

No React/SPA. Keep the dependency graph small; every new package needs a reason
tied to the current phase.

## Commands

```bash
docker compose up -d --wait               # infra: postgres, minio, kafka
uv run alembic upgrade head               # apply migrations
uv run alembic revision --autogenerate -m "..."  # then REVIEW the file
uv run uvicorn app.api.main:app --reload  # api on the host (hybrid dev)
docker compose --profile app up -d --build  # everything in containers
uv run pytest                             # unit tests
uv run pytest -m integration              # needs running infra
uv run ruff check . && uv run ruff format .
uv run python -m experiments.<name>       # phase experiments (experiments/)
```

Run lint + tests after every step.

## Code

- Full type hints. Config only via `app/config.py` (`FILEFLOW_*` env vars).
- Long-lived resources are created in the lifespan, stored on `app.state`,
  injected via `app/api/deps.py`. No module-level globals holding connections.
- Comment *why*, not *what* — especially transaction boundaries, delivery
  guarantees and failure behavior.
- Use enums for states (file/task/outbox status), never bare strings.
- Never swallow errors silently.
