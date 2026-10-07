#!/usr/bin/env bash
# Phase 09 experiment (Failure Lab test 7 + operational recovery).
#   experiments/phase09_dlq.sh
# Round 1: binary file            -> FAILED on attempt 1 -> dead letter "task_failed".
# Round 2: garbage in the topic   -> dead letter "invalid_message" (raw bytes kept).
# Round 3: MinIO down for good    -> gave up after 3 attempts -> dead letter;
#          MinIO comes back, operator redrives the task -> COMPLETED.
set -eu
cd "$(dirname "$0")/.."
export FILEFLOW_RETRY_BASE_DELAY_S=1 FILEFLOW_RETRY_BACKOFF_FACTOR=2 FILEFLOW_TASK_MAX_ATTEMPTS=3
q()   { docker compose exec -T postgres psql -U fileflow -tAc "$1"; }
dlq() { docker compose --profile app run --rm --no-deps -T worker python -m app.dlq "$@" 2>/dev/null; }
upload() {
  printf "$1" | curl -sf -F "file=@-;filename=exp9.txt" localhost:8000/files \
    | python3 -c 'import json,sys; print(json.load(sys.stdin)["task"]["id"])'
}
wait_status() { until [ "$(q "select status from tasks where id='$1'")" = "$2" ]; do sleep 0.5; done; }
show() { echo "  task: $(q "select status || ', attempt ' || attempt || '/' || max_attempts || coalesce(', ' || left(error, 70), '') from tasks where id='$1'")"; }

docker compose up -d --wait kafka-init >/dev/null 2>&1 || true
docker compose --profile app up -d --build --wait api >/dev/null
docker compose --profile app up -d outbox-publisher recovery worker >/dev/null

echo "=== Round 1: binary file"
T1=$(upload 'bin\0ary'); wait_status "$T1" FAILED; show "$T1"

echo "=== Round 2: poison message"
echo 'definitely not an event' | docker compose exec -T kafka /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9092 --topic file-processing
sleep 3

echo "=== Round 3: MinIO down until the task gives up"
docker compose --profile app stop worker >/dev/null 2>&1
T3=$(upload 'important data\n')
docker compose stop minio >/dev/null 2>&1
docker compose --profile app up -d --no-deps worker >/dev/null
wait_status "$T3" FAILED; show "$T3"

echo "=== DLQ (python -m app.dlq list):"
sleep 2  # let the outbox publisher relay the last dead letter
dlq list --limit 3 | sed 's/^/  /'

echo "=== MinIO is back; operator redrives the task"
docker compose start minio >/dev/null 2>&1; docker compose up -d --wait --no-deps minio >/dev/null 2>&1
dlq redrive "$T3" | sed 's/^/  /'
wait_status "$T3" COMPLETED; show "$T3"

unset FILEFLOW_RETRY_BASE_DELAY_S FILEFLOW_RETRY_BACKOFF_FACTOR FILEFLOW_TASK_MAX_ATTEMPTS
docker compose --profile app up -d >/dev/null 2>&1
