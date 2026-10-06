#!/usr/bin/env bash
# Phase 04 experiment: SIGKILL the worker while it processes a large file.
#   experiments/phase04_kill_worker.sh
# Expected: the task stays PROCESSING forever, even after the worker restarts —
# nobody ever claims a PROCESSING task. This is the problem leases solve (Phase 10-11).
set -eu
cd "$(dirname "$0")/.."
TMP=$(mktemp -d); trap 'rm -rf "$TMP"' EXIT
q() { docker compose exec -T postgres psql -U fileflow -tAc "$1"; }
NAME="exp4-kill-$(date +%s).bin"

docker compose --profile app up -d --build --wait api >/dev/null
docker compose --profile app up -d --scale worker=1 worker >/dev/null

head -c 1G /dev/urandom > "$TMP/big.bin"
TASK=$(curl -sf -F "file=@$TMP/big.bin;filename=$NAME" localhost:8000/files \
  | python3 -c 'import json,sys; print(json.load(sys.stdin)["task"]["id"])')
echo "task $TASK uploaded"

until [ "$(q "select status from tasks where id='$TASK'")" = "PROCESSING" ]; do sleep 0.1; done
sleep 0.5
docker compose kill -s SIGKILL worker >/dev/null
echo "worker SIGKILLed while processing"

docker compose --profile app up -d worker >/dev/null
sleep 5
echo "=== 5s after a fresh worker started:"
q "select status, attempt, worker_id, started_at, finished_at from tasks where id='$TASK'"
