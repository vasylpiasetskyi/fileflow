#!/usr/bin/env bash
# Phase 06 experiment (Failure Lab test 1): Kafka is down during uploads.
#   experiments/phase06_kafka_down.sh
# Expected (compare with Phase 05, where this lost events or lied with a 503):
#  - upload answers 201 at once: the API does not talk to Kafka;
#  - the event waits in outbox_events as PENDING, attempts/last_error grow;
#  - once Kafka is back the publisher relays it and the task gets COMPLETED.
set -eu
cd "$(dirname "$0")/.."
q() { docker compose exec -T postgres psql -U fileflow -tAc "$1"; }
NAME="exp6-kafka-down-$(date +%s).txt"

docker compose --profile app up -d --build --wait api >/dev/null
STARTED=$(date +%s)
docker compose --profile app up -d outbox-publisher worker >/dev/null
# Wait until the publisher is really running: one that cannot even start while
# Kafka is down only crash-loops, which would not show the relay retrying.
until docker compose logs outbox-publisher --since "$STARTED" 2>/dev/null | grep -q "outbox publisher started"; do sleep 0.5; done

docker compose stop kafka >/dev/null 2>&1
echo "kafka stopped"
echo "upload: $(echo hello | curl -s -o /dev/null -w 'HTTP %{http_code} in %{time_total}s' -F "file=@-;filename=$NAME" localhost:8000/files)"
EVENT="select o.status || ', attempts=' || o.attempts || ', error=' || coalesce(left(o.last_error, 60), '-')
       from outbox_events o join tasks t on o.key = t.id::text join files f on f.id = t.file_id where f.filename = '$NAME'"
sleep 15
echo "outbox after 15s without Kafka: $(q "$EVENT")"

docker compose start kafka >/dev/null 2>&1
docker compose up -d --wait --no-deps kafka >/dev/null 2>&1
echo "kafka is back"
until [ "$(q "select t.status from tasks t join files f on f.id=t.file_id where f.filename='$NAME'")" = "COMPLETED" ]; do sleep 1; done
echo "outbox: $(q "$EVENT")"
echo "task:   $(q "select t.status from tasks t join files f on f.id=t.file_id where f.filename='$NAME'")"
