#!/usr/bin/env bash
# Phase 08 experiment (Failure Lab tests 5 and 7): retries with backoff.
#   experiments/phase08_retries.sh
# Fast policy for the demo: base 2s, factor 2, 4 attempts (delays ~2s, 4s, 8s).
# Round 1: MinIO down for good      -> RETRY x3, then FAILED "gave up after 4 attempts".
# Round 2: MinIO back after a while -> RETRY, RETRY, then COMPLETED on a later attempt.
# Round 3: binary (unsupported) file -> FAILED on attempt 1, no retry (permanent).
set -eu
cd "$(dirname "$0")/.."
export FILEFLOW_RETRY_BASE_DELAY_S=2 FILEFLOW_RETRY_BACKOFF_FACTOR=2 FILEFLOW_TASK_MAX_ATTEMPTS=4
q() { docker compose exec -T postgres psql -U fileflow -tAc "$1"; }

upload() {  # $1 = content (printf format), prints the task id
  printf "$1" | curl -sf -F "file=@-;filename=exp8.txt" localhost:8000/files \
    | python3 -c 'import json,sys; print(json.load(sys.stdin)["task"]["id"])'
}
watch_task() {  # print every state change of task $1 until it is COMPLETED or FAILED
  local start last="" now
  start=$(date +%s)
  while :; do
    now=$(q "select status || ' attempt=' || attempt
                    || coalesce(' next_in=' || greatest(0, round(extract(epoch from next_attempt_at - now()))::int) || 's', '')
                    || coalesce(' error=' || left(error, 60), '')
             from tasks where id='$1'")
    if [ "${now%% next_in*}" != "${last%% next_in*}" ]; then
      printf '  t=%3ss  %s\n' "$(( $(date +%s) - start ))" "$now"; last=$now
    fi
    case "$now" in COMPLETED*|FAILED*) return;; esac
    sleep 0.5
  done
}

docker compose --profile app up -d --build --wait api >/dev/null
docker compose --profile app up -d outbox-publisher recovery >/dev/null

echo "=== Round 1: MinIO stays down"
docker compose --profile app stop worker >/dev/null 2>&1
TASK=$(upload 'round one\n')
docker compose stop minio >/dev/null 2>&1
docker compose --profile app up -d --no-deps worker >/dev/null
watch_task "$TASK"
docker compose start minio >/dev/null 2>&1; docker compose up -d --wait --no-deps minio >/dev/null 2>&1

echo "=== Round 2: MinIO comes back after ~5s"
docker compose --profile app stop worker >/dev/null 2>&1
TASK=$(upload 'round two\n')
docker compose stop minio >/dev/null 2>&1
docker compose --profile app up -d --no-deps worker >/dev/null
( sleep 5; docker compose start minio >/dev/null 2>&1; echo "  (minio started)" ) &
watch_task "$TASK"
wait

echo "=== Round 3: binary file"
TASK=$(upload 'binary\0content')
watch_task "$TASK"

# Back to the default retry policy.
unset FILEFLOW_RETRY_BASE_DELAY_S FILEFLOW_RETRY_BACKOFF_FACTOR FILEFLOW_TASK_MAX_ATTEMPTS
docker compose --profile app up -d >/dev/null 2>&1
