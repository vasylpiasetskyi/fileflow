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

One FastAPI application (REST API + Jinja2/HTMX UI). All async processing runs
in separate processes from the same codebase, each in its own container:

- `api` — FastAPI
- `worker` — Kafka consumer, processes files (N replicas)
- `outbox-publisher` — polls `outbox_events`, publishes to Kafka
- `recovery` — finds expired leases, requeues tasks

Infra: PostgreSQL (metadata + state + outbox), MinIO (blobs), Kafka (KRaft).

## Stack

Python 3.11+, FastAPI, Pydantic v2, SQLAlchemy 2.x (async), Alembic, asyncpg,
aiokafka, MinIO/S3, Jinja2 + HTMX + minimal CSS, pytest + pytest-asyncio,
Docker Compose. Optional: JSON logs, Prometheus, Grafana (only if it adds value).

## Phases (implementation order)

| #  | Step | Key content | Learn |
|----|------|-------------|-------|
| 01 | Foundation | layout, env config, compose (PG, MinIO, Kafka), `GET /health`, `GET /` | project structure, DI, lifespan, async DB, Docker networking |
| 02 | Upload + MinIO | `POST /files`, `GET /files/{id}`; metadata (id, filename, content_type, size, storage_key, checksum, created_at, status UPLOADING→UPLOADED); blobs never in PG | object storage, metadata/blob split, streaming uploads, storage abstraction |
| 03 | Task state machine | File + Task; PENDING→PROCESSING→COMPLETED/FAILED | domain model, state machine, service/repo split, tx boundaries |
| 04 | Basic worker | no Kafka yet; SHA-256, lines, words, size, processing time | background processing |
| 05 | Kafka | topic `file-processing`, msg `{event_id, task_id, file_id, attempt}`; producer, consumer group, offsets, graceful stop; 1→4 partitions/workers | consumer groups, partitioning, ordering, offsets, at-least-once, consumer death |
| 06 | Transactional Outbox | task + outbox event in one tx; publisher: poll PENDING → publish → mark PUBLISHED. Experiments: Kafka down after commit (stays PENDING); publish ok but DB update fails (republish — expected) | dual-write problem, eventual consistency, why exactly-once is hard |
| 07 | Idempotency | duplicate delivery after DB commit before Kafka commit; `UNIQUE(task_id)` on results, guarded state transitions; duplicate for COMPLETED = no-op | idempotency, races, constraints as correctness |
| 08 | Retry + backoff | 5s, 30s, 2m, 10m, then DLQ; exp backoff + jitter; retryable (timeouts, MinIO/DB/Kafka transient) vs non-retryable (corrupt, unsupported, invalid msg, missing object); PROCESSING→RETRY→PENDING | transient vs permanent, poison messages, backpressure |
| 09 | DLQ | topic `file-processing-dlq` with event_id, task_id, file_id, attempt, error, failed_at; UI shows FAILED/attempts/error | operational recovery, failure visibility |
| 10 | Lease + heartbeat | worker_id, lease_until = now+60s, periodic extension | leases, heartbeats, failure detection |
| 11 | Crash recovery | recovery finds PROCESSING AND lease_until < now → PENDING + requeue; experiment: `kill -9` worker | stale work, distributed coordination |
| 12 | Graceful shutdown | SIGTERM: stop consuming → finish current → commit offset → exit; otherwise lease expiry | cancellation, asyncio task mgmt, k8s-like termination |
| 13 | Large files | 1 GB / 5 GB, chunked streaming, never `await file.read()`; measure memory/time/throughput, compare with whole-file | async generators, chunk sizing, memory complexity |
| 14 | Concurrency | 1/4/8 workers, 100 files; throughput, avg/p95, queue latency, CPU, memory → report table | throughput vs latency, bottlenecks, diminishing returns |
| 15 | Observability | JSON logs with request_id, event_id, task_id, file_id, worker_id, attempt; metrics: files_uploaded_total, tasks_completed/failed/retried_total, processing_duration, queue_latency, active_tasks | correlation IDs, metrics vs logs |
| 16 | Minimal UI | upload + task table (file, status, attempt, time) + task details; HTMX polling, no SPA | just enough to visualize async/failures |
| 17 | Failure Lab | documented experiments (below) | |
| 18 | Benchmark | report | |
| 19 | Documentation | README: architecture, components, state machine, delivery semantics, failures, perf, trade-offs | |

## Failure Lab (mandatory)

1. Kafka down → DB + outbox committed, outbox stays PENDING, recovers later.
2. Worker crash during PROCESSING → lease expires → recovery → another worker → COMPLETED.
3. Duplicate message → one result.
4. Crash after Kafka publish → duplicate event → idempotent worker → one result.
5. MinIO unavailable → retry.
6. PostgreSQL unavailable → tx failure, no inconsistent state.
7. Corrupted file → FAILED → DLQ.
8. Outbox publisher crash: (a) before publish, (b) after publish, before marking PUBLISHED.

## Testing

Unit: state transitions, retry calc, message validation, idempotency, processing.
Integration: FastAPI + PostgreSQL + Kafka + MinIO.
Failure tests: actually kill services and verify recovery.

## README trade-offs to cover

Kafka vs RabbitMQ · PostgreSQL vs Redis · MinIO vs filesystem · Outbox vs direct
publish · at-least-once vs exactly-once · polling vs CDC · sync vs async processing.

## State machine (target)

```text
PENDING → PROCESSING → COMPLETED
              ↘ RETRY → PENDING
PROCESSING → FAILED → DLQ
```

Delivery semantics: **at-least-once delivery + idempotent processing.**
