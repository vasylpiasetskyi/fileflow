#!/usr/bin/env bash
# Phase 06 experiment (Failure Lab test 8): the outbox publisher crashes
#   a) before sending to Kafka, b) after Kafka acked but before marking PUBLISHED.
#   experiments/phase06_publisher_crash.sh
# Expected:
#  a) nothing in Kafka, row still PENDING -> a restarted publisher sends it once;
#  b) message in Kafka, row still PENDING -> a restarted publisher sends it AGAIN:
#     two copies with the same event_id. The worker processes the first and
#     skips the second (task no longer PENDING): one result.
set -eu
cd "$(dirname "$0")/.."
q() { docker compose exec -T postgres psql -U fileflow -tAc "$1"; }
copies() {  # number of messages in the topic carrying event id $1
  docker compose exec -T kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
    --topic file-processing --from-beginning --timeout-ms 5000 2>/dev/null | grep -c "$1" || true
}
crash_round() {  # $1 = crash point
  local name="exp6-crash-$1-$(date +%s).txt"
  echo hello | curl -sf -o /dev/null -F "file=@-;filename=$name" localhost:8000/files
  local event task
  event=$(q "select o.id from outbox_events o join tasks t on o.key=t.id::text join files f on f.id=t.file_id where f.filename='$name'")
  task=$(q "select t.id from tasks t join files f on f.id=t.file_id where f.filename='$name'")
  echo "=== crash point: $1 (event $event)"

  docker compose --profile app run --rm -e FILEFLOW_OUTBOX_CRASH_POINT="$1" outbox-publisher >/dev/null 2>&1 || true
  echo "after crash:   outbox=$(q "select status from outbox_events where id='$event'"), copies in kafka=$(copies "$event")"

  docker compose --profile app run --rm -d outbox-publisher >/dev/null 2>&1
  until [ "$(q "select status from outbox_events where id='$event'")" = "PUBLISHED" ]; do sleep 0.5; done
  docker compose --profile app stop outbox-publisher >/dev/null 2>&1 || true
  docker ps -q --filter "name=fileflow-outbox-publisher-run" | xargs -r docker stop >/dev/null
  until [ "$(q "select status from tasks where id='$task'")" = "COMPLETED" ]; do sleep 0.5; done
  echo "after restart: outbox=PUBLISHED, copies in kafka=$(copies "$event"), task=$(q "select status from tasks where id='$task'"), results=$(q "select count(*) from task_results where task_id='$task'")"
}

docker compose --profile app up -d --build --wait api >/dev/null
docker compose --profile app up -d worker >/dev/null
docker compose --profile app stop outbox-publisher >/dev/null 2>&1 || true

crash_round before_publish
crash_round after_publish
echo "=== worker log for the duplicate:"
docker compose logs worker --since 2m --no-log-prefix 2>/dev/null | grep "skipping event" | tail -1
