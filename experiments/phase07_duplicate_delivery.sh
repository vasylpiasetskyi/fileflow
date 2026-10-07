#!/usr/bin/env bash
# Phase 07 experiment (Failure Lab test 3): the worker dies after the task's
# outcome is committed to PostgreSQL but before the Kafka offset is committed.
#   experiments/phase07_duplicate_delivery.sh
# Expected: Kafka redelivers the message (lag stays 1); the restarted worker
# recognises it as a duplicate (task already COMPLETED) and only commits the
# offset. One task, one attempt, one result.
set -eu
cd "$(dirname "$0")/.."
q()   { docker compose exec -T postgres psql -U fileflow -tAc "$1"; }
lag() { docker compose exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
          --describe --group fileflow-workers 2>/dev/null | awk 'NR>1 && $6 ~ /^[0-9]+$/ {s += $6} END {print s+0}'; }
state() { echo "task=$(q "select status || ', attempt=' || attempt from tasks where id='$1'"), results=$(q "select count(*) from task_results where task_id='$1'"), group lag=$(lag)"; }

docker compose --profile app up -d --build --wait api >/dev/null
docker compose --profile app up -d outbox-publisher >/dev/null
docker compose --profile app stop worker >/dev/null 2>&1 || true
until [ "$(lag)" = "0" ]; do sleep 0.5; done  # start from a drained topic

TASK=$(echo "process me once" | curl -sf -F "file=@-;filename=exp7.txt" localhost:8000/files \
  | python3 -c 'import json,sys; print(json.load(sys.stdin)["task"]["id"])')
until [ "$(lag)" = "1" ]; do sleep 0.5; done
echo "task $TASK queued:       $(state "$TASK")"

docker compose --profile app run --rm -e FILEFLOW_WORKER_CRASH_POINT=after_processing worker >/dev/null 2>&1 || true
echo "worker crashed:          $(state "$TASK")"

STARTED=$(date +%s)
docker compose --profile app up -d worker >/dev/null
until docker compose logs worker --since "$STARTED" 2>/dev/null | grep -q "$TASK"; do sleep 0.5; done
until [ "$(lag)" = "0" ]; do sleep 0.5; done
echo "after redelivery:        $(state "$TASK")"
echo "=== restarted worker log:"
docker compose logs worker --since "$STARTED" --no-log-prefix 2>/dev/null | grep "$TASK" | cut -c1-200
