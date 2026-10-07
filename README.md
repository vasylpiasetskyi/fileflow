# FileFlow

Asynchronous file processing system built as a learning lab for backend and
distributed-systems patterns: transactional outbox, at-least-once delivery
with idempotent consumers, guarded state transitions with a fencing token,
retries with backoff, a dead letter queue, and end-to-end streaming of large
files.

Stack: Python 3.11, FastAPI, PostgreSQL, MinIO (S3), Kafka (KRaft),
SQLAlchemy 2 (async) + Alembic, aiokafka, aiobotocore, Jinja2 + HTMX, Docker
Compose.

> **Status: done** — phases 01–09, 11 and 14 of [docs/PLAN.md](docs/PLAN.md).
> Leases / crash recovery, observability, the benchmark and the Failure Lab
> write-up are a documented perspective, not implemented.

## What it does

A client uploads a file. FileFlow stores the bytes in MinIO and the metadata
in PostgreSQL, creates a processing task, and a pool of workers processes it
asynchronously: SHA-256 (verified against the upload), size, line and word
count. Clients follow the task through the REST API or the web UI. Transient
failures are retried with backoff; tasks that fail for good land in a dead
letter queue from which an operator can send them back.

## Architecture

```text
                 ┌──────────────────────────────────────────────┐
  browser / curl │ api (FastAPI: REST + HTMX UI)                 │
  ─────────────▶ │  upload: stream bytes → MinIO                 │
                 │  one tx: file UPLOADED + task + outbox event  │
                 └──────────┬───────────────────────┬────────────┘
                            │ SQL                   │ S3
                            ▼                       ▼
                     ┌────────────┐           ┌──────────┐
                     │ PostgreSQL │           │  MinIO   │
                     │ files      │           │  blobs   │
                     │ tasks      │           └──────────┘
                     │ results    │                 ▲
                     │ outbox     │                 │ stream read
                     └─────┬──────┘                 │
          poll PENDING rows│      ▲ claim / finish  │
                           ▼      │ task (guarded)  │
               ┌──────────────────┴─┐         ┌─────┴──────────┐
               │ outbox-publisher   │         │ worker × N     │
               │ outbox → Kafka     │         │ consumer group │
               └─────────┬──────────┘         └───────▲────────┘
                         │   topic file-processing     │
                         └──────────▶ Kafka ───────────┘
                                       │ topic file-processing-dlq
               ┌─────────────────┐     ▼
               │ recovery        │   dead letters (failed tasks,
               │ RETRY → PENDING │   rejected messages)
               │ (+ outbox event)│
               └─────────────────┘
```

One codebase and one Docker image (`fileflow-app`), several processes:

| Process | Command | Talks to | Role |
|---|---|---|---|
| `api` | `uvicorn app.api.main:app` | PostgreSQL, MinIO | REST + UI; writes events to the outbox, never to Kafka |
| `outbox-publisher` | `python -m app.outbox` | PostgreSQL, Kafka | relays `outbox_events` rows to Kafka |
| `worker` (×N) | `python -m app.worker` | Kafka, PostgreSQL, MinIO | consumer group `fileflow-workers`; processes files |
| `recovery` | `python -m app.recovery` | PostgreSQL | requeues `RETRY` tasks when due (via the outbox) |
| `migrate` | `alembic upgrade head` | PostgreSQL | one-shot, before the others (compose) |
| `kafka-init`, `minio-init` | — | Kafka, MinIO | one-shot: create topics and the bucket |

## Run

**Hybrid dev** — infrastructure in Docker, Python on the host:

```bash
docker compose up -d --wait          # postgres, minio (+bucket), kafka (+topics)
uv sync
uv run alembic upgrade head
uv run uvicorn app.api.main:app --reload   # terminal 1
uv run python -m app.outbox                # terminal 2
uv run python -m app.worker                # terminal 3
uv run python -m app.recovery              # terminal 4
open http://localhost:8000
```

**Everything in containers:**

```bash
docker compose --profile app up -d --build
docker compose --profile app up -d --scale worker=3   # useful up to the partition count
docker compose --profile tools up -d adminer          # optional DB web UI
```

| Service | Host address | In-network address |
|---|---|---|
| API + UI | http://localhost:8000 | `api:8000` |
| PostgreSQL | localhost:5432 (fileflow / fileflow) | `postgres:5432` |
| MinIO S3 / console | http://localhost:9000 / http://localhost:9001 (fileflow / fileflow-secret) | `minio:9000` |
| Kafka | localhost:9092 | `kafka:19092` |
| Adminer (`--profile tools`) | http://localhost:8080 (server `postgres`, fileflow / fileflow) | — |

## API

| Method & path | Purpose |
|---|---|
| `POST /files` | multipart upload (`file` field) → `201 {file, task}` |
| `POST /files/stream?filename=` | raw-body upload, streamed end to end → `201 {file, task}` |
| `GET /files/{id}` | file metadata |
| `GET /tasks/{id}` | task status, attempts, error, result |
| `GET /health` | per-dependency checks (PostgreSQL, MinIO, Kafka); 503 if any is down |
| `GET /health/live` | liveness only, no dependency checks |
| `GET /` and `/ui/...` | HTMX UI (HTML fragments) |

```bash
curl -F "file=@notes.txt" localhost:8000/files
curl -X POST -T big.txt -H 'Content-Type: text/plain' 'localhost:8000/files/stream?filename=big.txt'
curl localhost:8000/tasks/<task_id>
```

Errors: `413` upload larger than `max_upload_bytes` (10 GiB), `503` object
storage unavailable, `404` unknown id.

## How it works

### Upload: row first, blob second

```text
tx1: INSERT files (UPLOADING)
     stream bytes → MinIO (S3 multipart, 8 MiB parts), SHA-256 on the fly
tx2: files → UPLOADED + INSERT tasks (PENDING) + INSERT outbox_events — all or nothing
```

The row is written before the bytes, so an interrupted upload is visible as an
`UPLOADING` row instead of an unknown orphan object. Any error while receiving
the body (storage down, client gone, too large) aborts the multipart upload
and marks the file `FAILED`. The object becomes visible in MinIO only when the
multipart upload completes.

### Transactional outbox

The API never talks to Kafka. The event announcing a task is a row in
`outbox_events`, written in the same transaction as the task — "the task
exists" and "its event will be sent" cannot disagree. The outbox publisher
claims `PENDING` rows (`FOR UPDATE SKIP LOCKED`), sends them (key =
`task_id`), and marks the acknowledged ones `PUBLISHED` in the same
transaction. Kafka down → rows wait (`attempts`, `last_error`), nothing is
lost, uploads keep working. A publisher crash after sending but before
committing re-sends the row: a duplicate with the same `event_id` (the outbox
row id *is* the `event_id`).

### Kafka and workers: at-least-once

Workers form one consumer group; each partition is read by exactly one worker,
so partitions cap parallelism. Auto-commit is off: a worker commits a
message's offset only after the task's outcome is stored in PostgreSQL. Per
message:

| Situation | Action |
|---|---|
| processed, or a harmless duplicate | commit |
| invalid message / unknown task | dead letter to the DLQ, *then* commit |
| infrastructure error (DB down, DLQ unreachable) | no commit, seek back, retry after a pause |

### Idempotent processing

Duplicates are expected (Kafka redelivery, outbox re-publish).
`ProcessingService.process()` returns an explicit outcome instead of an error:
`processed`, `duplicate` (task already finished), `in_progress` (another worker
holds it), `scheduled` (it waits for a retry) or `stale` (we lost it mid-way).
Three guards:

1. **State machine** — only a `PENDING` task can be claimed; transitions are
   guarded updates: `UPDATE … WHERE status IN (<allowed sources>)`.
2. **Fencing token** — `start()` increments `attempt`; `complete()`/`fail()`
   must present it (`… AND attempt = :expected`), so a previous owner can
   never overwrite the current one's work.
3. **`UNIQUE(task_results.task_id)`** — the last line of defense.

`task_id` is the natural idempotency key, so there is no inbox table.

### Retries

Failures are classified (`app/retry.py`): permanent — corrupted object
(checksum mismatch), binary file, missing object or file row → `FAILED` at
once. Everything else (MinIO / DB / network trouble, unknown errors) is
retryable: while attempts are left the task moves to `RETRY` with
`next_attempt_at = now() + min(max, base · factor^(n−1)) ± 20 %` (≈ 5 s, 25 s,
2 min, 10 min; 5 attempts per task, `tasks.max_attempts`). The **recovery**
process finds due `RETRY` tasks and, in one transaction, moves them to
`PENDING` and writes a new event to the outbox. The delay lives in the
database, on the database clock: no sleeping workers, no retry topics.

### Dead letter queue and redrive

Topic `file-processing-dlq`. Each `DeadLetter` is self-contained: `kind`
(`task_failed` / `invalid_message` / `unknown_task`), `task_id`, `file_id`,
`attempt`, `error`, `failed_at`, and for rejected messages `source`
(`topic[partition]@offset`) and `raw_value`. The rule: a decision is committed
only together with its dead letter —

- a task ending `FAILED`: the dead letter is an outbox row in the **same
  transaction** as `FAILED`;
- a message the worker rejects: the worker publishes the dead letter
  **before** committing the offset.

After fixing the cause, an operator sends the task back:

```bash
uv run python -m app.dlq list              # recent dead letters (commits nothing)
uv run python -m app.dlq redrive TASK_ID   # or the Redrive button in the UI
```

Redrive (`FAILED → PENDING`, a manual-only transition) never resets
`attempt` — it is the fencing token and must only grow — it raises the task's
`max_attempts` instead.

### Large files

`POST /files` (multipart) is what a browser form sends; Starlette spools the
whole body to a temp file before the endpoint runs. `POST /files/stream`
passes `request.stream()` — an async generator, not read in the endpoint — down
a pipeline of generators (size limit → SHA-256 → 8 MiB rechunking → S3 part
upload). Bytes are pulled from the socket only as fast as MinIO accepts parts,
so TCP backpressure reaches the client and at most one part is in memory.
Workers read objects back in 1 MiB chunks and count words across chunk
boundaries.

Measured with a 1 GiB text file (`uv run python -m experiments.phase11_large_files`,
laptop, compose):

| Upload | Time | API cgroup memory | API process RSS | Temp disk | `/health/live` p99 / max |
|---|---|---|---|---|---|
| multipart | 33–43 s | ~1.2 GiB | 180 MiB | 1 GiB | 14 / 31 ms |
| stream | 21–27 s | ~150 MiB | 156 MiB | 0 | 12 / 20 ms |

- The multipart "memory" is the kernel page cache of the spooled file, charged
  to the container's cgroup — not the process, but it counts against
  container memory limits.
- Hashing in the event loop adds ~10 ms at p99 (idle: 6 ms): measured, not
  worth moving to a thread.
- Worker: 1 GiB processed in ~6.5 s with a flat ~90 MiB. Reading the whole
  file into memory first peaks at **~9.9 GiB** RSS (`bytes.split()`
  materializes every word); streaming stays at ~45 MiB.

### UI

`http://localhost:8000` — one page, server-rendered with Jinja2, updated with
HTMX: dependency badges, upload form, task table (newest first, status
filter, one joined query), task details (attempts, worker, timings, error,
result) and a Redrive button for `FAILED` tasks. The table polls every 2 s and
refreshes immediately when a response carries `HX-Trigger: tasks-changed`.
UI routes call the same services as the REST API and answer expected errors
with `200` + a message, because HTMX does not swap 4xx/5xx responses.

## Task state machine

```text
PENDING ──> PROCESSING ──> COMPLETED
   ▲                 ├───> FAILED ──(manual redrive)──> PENDING
   │                 └───> RETRY ─┐
   └──────────────────────────────┘  (recovery, when next_attempt_at is due)
```

Defined once as data in `app/task_state_machine.py`; repositories turn it into
guarded updates, so checking a rule and applying it is one atomic statement.

## Guarantees

- **No lost work** once an upload returns `201`: the task and its event are
  committed together; the outbox survives Kafka outages; offsets are
  committed only after outcomes are stored.
- **At-least-once delivery, exactly one result per task**: duplicates are
  turned into no-ops by the state machine, the fencing token and the unique
  result row.
- **Transient failures are retried** with exponential backoff and jitter;
  permanent ones fail fast and are dead-lettered.

## Experiments

Each script breaks something on purpose and prints what happened
(`experiments/`, run against the compose stack):

| Script | Shows |
|---|---|
| `phase02_crash_upload.sh` | `kill -9` mid-upload → `UPLOADING` row, no object, incomplete multipart |
| `phase03_claim_race.py` | 10 concurrent claims: naive read-check-write → 10 winners, guarded update → 1 |
| `phase05_partitions.sh` | 4 workers on 1 partition → one does all the work; on 4 partitions → spread |
| `phase05_kill_consumer.sh` | consumer killed mid-task → redelivery after the session timeout; task stuck in `PROCESSING` |
| `phase06_kafka_down.sh` | Kafka down → upload still `201`, event waits in the outbox, delivered later |
| `phase06_publisher_crash.sh` | publisher crash before / after publish → resent once / duplicate, one result |
| `phase07_duplicate_delivery.sh` | worker dies after DB commit, before offset commit → redelivery is a no-op |
| `phase08_retries.sh` | MinIO down → retries with growing delays → gave up / recovered; binary file → no retry |
| `phase09_dlq.sh` | dead letters for a failed task and a poison message; redrive → `COMPLETED` |
| `phase11_large_files.py` | 1 GiB: multipart vs stream, memory, temp disk, event-loop latency, chunk sizes |

## Tests

```bash
uv run pytest                    # unit tests, no infrastructure
uv run pytest -m integration     # against the compose stack
uv run ruff check . && uv run ruff format .
```

Integration tests use their own database (`fileflow_test`, truncated before
each test) and a fresh Kafka topic + DLQ + consumer group per test.

## Operations

```bash
uv run python -m app.dlq list | redrive TASK_ID
K=/opt/kafka/bin
docker compose exec kafka $K/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group fileflow-workers
docker compose exec kafka $K/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic file-processing-dlq --from-beginning
docker compose exec postgres psql -U fileflow -c "select status, count(*) from tasks group by 1"
```

Fast retry policy for experiments:
`FILEFLOW_RETRY_BASE_DELAY_S=2 FILEFLOW_RETRY_BACKOFF_FACTOR=2 docker compose --profile app up -d`.
All settings are `FILEFLOW_*` environment variables (`app/config.py`).

## Layout

```text
app/
  api/            FastAPI app: main.py (lifespan), deps.py (DI), routes/ (health, files, tasks, ui)
  services/       use cases, own transaction boundaries: file, task, processing, outbox relay, recovery
  repositories/   SQL per table, never commit; guarded_update.py = the atomic transition
  models.py       ORM models / schema          task_state_machine.py  allowed transitions
  processing.py   streaming analysis (pure)    retry.py               error classification + backoff (pure)
  events.py       Kafka message schemas        storage.py, kafka.py   S3 and Kafka adapters
  worker/ outbox/ recovery/ dlq/               process entrypoints and the operator CLI
  failure_lab.py  crash points for experiments config.py, db.py       settings, engine
templates/ static/  Jinja2 + HTMX UI
migrations/         Alembic (async; every autogenerated revision is reviewed)
experiments/        failure and measurement scripts
tests/              unit; tests/integration needs the compose stack
docs/PLAN.md        phases: what was built, what is deferred and why
```

## Known limitations

- **No leases**: a worker killed mid-task (or a database outage right after a
  task was claimed) leaves the task in `PROCESSING` forever; redelivered
  events are answered `in_progress`. The recovery process and the fencing
  token are ready for leases, but they are not implemented
  (`docs/PLAN.md`, Deferred).
- Nothing cleans up files stuck in `UPLOADING` or incomplete multipart
  uploads; `PUBLISHED` outbox rows are never deleted.
- The API's `/health` still probes Kafka, which the API itself no longer needs.
- No metrics, structured logs or benchmark yet (deferred).

## Trade-offs worth knowing

- **Outbox vs publishing directly from the API** — one atomic write instead
  of a dual write; costs a publisher process, polling latency (≤ 0.5 s) and
  possible duplicates.
- **Polling the outbox vs CDC (Debezium)** — simple and enough here; CDC
  reads the WAL instead and scales better.
- **Retry schedule in the database vs retry topics** — one mechanism (the
  outbox) and queryable state, instead of one topic and consumer per delay.
- **Failed status in the database vs a DLQ topic** — for tasks the `FAILED`
  rows already are the dead letters; the topic is what carries messages that
  have no row (invalid JSON, unknown task) and what other services could
  subscribe to.
- **At-least-once + idempotency vs "exactly once"** — exactly-once across two
  systems (PostgreSQL and Kafka) reduces to at-least-once plus deduplication.
- **Streaming through the API vs presigned URLs** — the API sees every byte
  (checksum, size limit) at the cost of bandwidth; presigned uploads go
  straight to S3 but move those checks elsewhere.
