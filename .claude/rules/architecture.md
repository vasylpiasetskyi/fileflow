---
name: architecture
description: Layer boundaries and distributed-systems invariants for FileFlow
alwaysApply: true
---

# Architecture Rules

## Layers

- `app/api/` — HTTP and UI only. No business logic, no raw SQL.
- services — business logic and transaction boundaries (who opens/commits a tx).
- repositories — SQL for one aggregate; never commit on their own.
- `app/storage.py`, `app/kafka.py` — infrastructure adapters, no domain logic.
- worker / outbox publisher / recovery — process entrypoints that call the same
  services; never duplicate logic between them and the API.

## Invariants

- Never dual-write: a DB change and its event are written in **one** DB
  transaction (outbox); only the outbox publisher talks to Kafka for those events.
- Delivery is at-least-once. Every consumer must be idempotent; duplicates are
  expected, not exceptional.
- State changes go through explicit, guarded transitions
  (`UPDATE ... WHERE status = <expected>`), backed by DB constraints.
- Blobs live in object storage; PostgreSQL stores metadata only.
- Stream large files in chunks; never read a whole file into memory.
- Every process handles SIGTERM gracefully.
