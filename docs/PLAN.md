# FileFlow — Implementation & Learning Plan

Production-inspired asynchronous file processing system, built as a
Senior Backend / Distributed Systems learning lab (not a frontend project).

## Ground rule

Do not implement the entire project at once. Implement phase by phase.
Before each phase, explain the architectural decision and what to learn from it.
After implementation, provide tests and a small failure/experiment scenario.
Do not hide important implementation details behind abstractions or libraries.
Prefer explicit, understandable code over premature abstraction.

## Architecture

One codebase, one Docker image, several processes:

- `api` — FastAPI: REST + Jinja2/HTMX UI; writes to PostgreSQL + MinIO only
- `outbox-publisher` — relays `outbox_events` rows to Kafka
- `worker` (N replicas) — Kafka consumer group, processes files
- `recovery` — requeues tasks when due (retries now; expired leases next)

Infra: PostgreSQL (metadata, task state, outbox), MinIO (blobs), Kafka (KRaft,
topics `file-processing` + `file-processing-dlq`).

Delivery semantics: **at-least-once delivery + idempotent processing.**

## Stack

Python 3.11, FastAPI, Pydantic v2, SQLAlchemy 2.x (async), Alembic, asyncpg,
aiokafka, aiobotocore (S3), Jinja2 + HTMX + minimal CSS, pytest +
pytest-asyncio, uv, Ruff, Docker Compose. Optional: Prometheus, Grafana (only
if it adds value).

## Done

| #  | Phase | What was built | Experiments |
|----|-------|----------------|-------------|
| 01 | Foundation | layout, env config, compose (PG, MinIO, Kafka KRaft), lifespan + DI, `/health` with per-dependency probes | PG stop/start, API survives |
| 02 | Upload + MinIO | `POST /files`, `GET /files/{id}`; row first, blob second; S3 multipart streaming upload; `UPLOADING → UPLOADED / FAILED`; Alembic | MinIO down → FAILED; `kill -9` mid-upload → orphan `UPLOADING` + incomplete multipart |
| 03 | Task state machine | `tasks`; transitions as data; guarded `UPDATE … WHERE status IN …`; file `UPLOADED` + task in one tx | naive vs guarded claim race |
| 04 | Basic worker | polling worker (`FOR UPDATE SKIP LOCKED`), streaming analysis (SHA-256, size, lines, words), checksum check, `task_results`; separate test DB | workers sharing tasks; `kill -9` → stuck `PROCESSING` |
| 05 | Kafka | producer (`acks=all`, idempotent), consumer group, manual offset commit, seek-back on infra errors; polling removed | partitions cap parallelism; consumer kill → redelivery after session timeout; Kafka down → lost/late events (dual write) |
| 06 | Transactional outbox | event row in the upload tx; relay `SKIP LOCKED` → Kafka → `PUBLISHED`; API has no Kafka client | Kafka down → 201, event waits; publisher crash before/after publish → duplicate (Failure Lab 1, 8) |
| 07 | Idempotency | explicit outcomes (processed / duplicate / in_progress / stale); fencing token (`attempt`) on complete/fail; `UNIQUE(task_id)` | crash after DB commit before offset commit → no-op redelivery (Failure Lab 3, 4) |
| 08 | Retries | error classification; `PROCESSING → RETRY → PENDING`; exp. backoff + jitter (`next_attempt_at`, DB clock); `recovery` process requeues via outbox | MinIO down → retries then gave up / recovers; binary file → no retry (Failure Lab 5) |
| 09 | DLQ | `DeadLetter` (task_failed / invalid_message / unknown_task); FAILED + dead letter in one tx (outbox); rejected messages published before offset commit; per-task `max_attempts`; `python -m app.dlq list / redrive` | binary file, poison message, give-up + redrive → COMPLETED (Failure Lab 7) |

## Remaining (revised)

The original phases 10–19 were merged where they only make sense together:
10+11+12 (a lease is useless without recovery; shutdown is half done),
14+18 (concurrency and benchmark are one experiment), 17+19 (most Failure Lab
scenarios already exist as `experiments/`). Observability moves before the
benchmark, so there is something to measure with.

| #  | Phase | Key content | Learn |
|----|-------|-------------|-------|
| 10 | Leases, heartbeat, crash recovery, graceful shutdown | `lease_until` set on start and extended by a heartbeat task while processing; complete/fail also check the lease; `recovery` moves `PROCESSING AND lease_until < now()` back to `PENDING` (+ event via outbox); fencing token protects against the zombie owner. SIGTERM: stop consuming, finish the current task within a timeout, otherwise cancel and release the lease; commit, leave the group. Experiments: `kill -9` worker → another worker completes (Failure Lab 2); SIGTERM mid-task | leases vs locks, heartbeats, failure detection, stale work, cancellation, asyncio task management, k8s-like termination |
| 11 | Large files | true streaming upload from `request.stream()` (no spool to disk), compared with the current multipart/spooled path; 1 GB (5 GB optional, mind the disk); measure memory, time, throughput; chunk size effects | streaming end to end, async generators, memory complexity, backpressure, chunk sizing |
| 12 | Observability | JSON logs with correlation ids (request_id, event_id, task_id, file_id, worker_id, attempt) propagated API → outbox → Kafka → worker; Prometheus metrics: files_uploaded_total, tasks_completed/failed/retried_total, processing_duration, queue_latency, outbox lag, DLQ size, active_tasks. Grafana optional | correlation ids, metrics vs logs, debugging distributed flows |
| 13 | Concurrency + benchmark | 1 / 4 / 8 workers (and partitions), 100 files; throughput, avg / p95 latency, queue latency, CPU, memory → report table; find the bottleneck | throughput vs latency, horizontal scaling, diminishing returns, bottleneck analysis |
| 14 | Minimal UI | one page: upload, task table (file, status, attempt, time), task details (worker, error, result); HTMX polling; no SPA | just enough to see async processing and failures |
| 15 | Failure Lab + documentation | add the missing scenarios (6: PostgreSQL unavailable; 2 is covered in phase 10); one document with every experiment and its observed result; README: architecture diagram, guarantees, trade-offs; close the backlog below | explaining and defending the design |

## Failure Lab

| # | Scenario | Status |
|---|----------|--------|
| 1 | Kafka down → upload succeeds, outbox `PENDING`, delivered later | ✅ `experiments/phase06_kafka_down.sh` |
| 2 | Worker crash during `PROCESSING` → lease expires → recovery → another worker → `COMPLETED` | ⏳ phase 10 |
| 3 | Duplicate message → one result | ✅ `experiments/phase07_duplicate_delivery.sh` |
| 4 | Crash after Kafka publish → duplicate event → one result | ✅ `experiments/phase06_publisher_crash.sh` |
| 5 | MinIO unavailable → retry | ✅ `experiments/phase08_retries.sh` |
| 6 | PostgreSQL unavailable → tx failure, no inconsistent state | ⏳ phase 15 |
| 7 | Corrupted / unsupported file → `FAILED` → DLQ | ✅ `experiments/phase09_dlq.sh` |
| 8 | Outbox publisher crash before / after publish | ✅ `experiments/phase06_publisher_crash.sh` |

## Backlog (small fixes, close by phase 15)

- `/health` of the API still probes Kafka, which the API no longer needs
  (an outage would mark a working API unhealthy); split liveness/readiness.
- Outbox relay docstring claims "commit order"; it is creation (`now()` at
  transaction start) order — fix the wording.
- `outbox_events.last_error` is not cleared after a successful publish.
- Nothing cleans up files stuck in `UPLOADING` or incomplete multipart uploads
  in MinIO (lifecycle rule + a reaper).
- No retention for `PUBLISHED` outbox rows.

## Testing

Unit: state transitions, retry policy, message validation, idempotency, processing.
Integration (`-m integration`, separate `fileflow_test` DB, a Kafka topic per
test): FastAPI + PostgreSQL + Kafka + MinIO.
Failure tests: `experiments/` scripts that actually kill or stop services.

## README trade-offs to cover

Kafka vs RabbitMQ · PostgreSQL vs Redis · MinIO vs filesystem · outbox vs direct
publish · polling vs CDC · at-least-once vs exactly-once · sync vs async
processing · DLQ topic vs failed status in the database · retry topics vs
scheduled retries in the database.

## State machine (current)

```text
PENDING ──> PROCESSING ──> COMPLETED
   ▲                 ├───> FAILED ──(manual redrive)──> PENDING
   │                 └───> RETRY ─┐
   └──────────────────────────────┘  (recovery, when next_attempt_at is due)
```

Phase 10 adds `PROCESSING ──(lease expired, recovery)──> PENDING`.
