#!/usr/bin/env bash
# Phase 05 experiment: SIGKILL the worker mid-processing.
#   experiments/phase05_kill_consumer.sh
# Expected:
#  - the offset was not committed, so Kafka redelivers the message (at-least-once);
#  - redelivery waits for the dead member's session timeout (no LeaveGroup sent);
#  - the redelivered event is skipped (task is PROCESSING, not PENDING), so the
#    task stays stuck in PROCESSING. Leases fix this (Phase 10-11).
set -eu
cd "$(dirname "$0")/.."
TMP=$(mktemp -d); trap 'rm -rf "$TMP"' EXIT
q() { docker compose exec -T postgres psql -U fileflow -tAc "$1"; }

docker compose --profile app up -d --build --wait api >/dev/null
docker compose --profile app up -d --scale worker=1 worker >/dev/null

head -c 1G /dev/urandom > "$TMP/big.bin"
TASK=$(curl -sf -F "file=@$TMP/big.bin;filename=exp5-kill.bin" localhost:8000/files \
  | python3 -c 'import json,sys; print(json.load(sys.stdin)["task"]["id"])')
echo "task $TASK uploaded"

until [ "$(q "select status from tasks where id='$TASK'")" = "PROCESSING" ]; do sleep 0.1; done
sleep 0.5
docker compose kill -s SIGKILL worker >/dev/null
KILLED=$(date +%s)
echo "worker SIGKILLed while processing"

docker compose --profile app up -d worker >/dev/null
until docker compose logs worker --since "${KILLED}" 2>/dev/null | grep -q "$TASK"; do sleep 0.5; done
echo "=== message redelivered $(( $(date +%s) - KILLED ))s after the kill:"
docker compose logs worker --since "${KILLED}" --no-log-prefix 2>/dev/null | grep "$TASK" | tail -1
echo "=== task:"
q "select status, attempt, worker_id from tasks where id='$TASK'"
