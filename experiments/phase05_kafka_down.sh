#!/usr/bin/env bash
# Phase 05 experiment: the dual write. Kafka is down when a file is uploaded.
#   experiments/phase05_kafka_down.sh
# Expected: the client gets 503, yet file UPLOADED + task PENDING are already
# committed. What happens to the event is UNKNOWN to the API:
#  - if the producer could not even fetch metadata, the send fails after
#    request_timeout_ms (~5s) and the event never existed -> task PENDING forever;
#  - if the event was already buffered, publish() times out (~10s) but the
#    producer keeps retrying and delivers it once Kafka is back -> the task is
#    processed after all, although the client was told it failed;
#  - if the API dies while the event is buffered (round 2), it is lost.
# Observed runs produced both "PENDING forever" and "COMPLETED after a 503".
# Either way the client cannot know. The transactional outbox (Phase 06) fixes this.
# Run alone: other compose commands may restart Kafka as a dependency.
set -eu
cd "$(dirname "$0")/.."
q() { docker compose exec -T postgres psql -U fileflow -tAc "$1"; }
NAME="exp5-kafka-down-$(date +%s).txt"

docker compose --profile app up -d --build --wait api >/dev/null
docker compose --profile app up -d worker >/dev/null

attempt() {  # $1 = filename, $2 = restart api while kafka is down? (yes/no)
  docker compose stop kafka >/dev/null 2>&1
  echo "kafka stopped"
  echo "upload: $(echo 'hello' | curl -s -o /dev/null -w 'HTTP %{http_code} in %{time_total}s' -F "file=@-;filename=$1" localhost:8000/files)"
  if [ "$2" = yes ]; then
    docker compose kill -s SIGKILL api >/dev/null 2>&1
    echo "api killed (its producer buffer is gone)"
  fi
  docker compose start kafka >/dev/null 2>&1
  docker compose up -d --wait --no-deps kafka >/dev/null 2>&1
  [ "$2" = yes ] && docker compose --profile app up -d --wait --no-deps api >/dev/null 2>&1
  echo "kafka is back; giving the worker 20s..."
  sleep 20
  q "select 'file ' || f.status || ', task ' || t.status from files f join tasks t on t.file_id=f.id where f.filename='$1'"
}

echo "=== Round 1: API keeps running"
attempt "$NAME-a" no
echo "=== Round 2: API restarted while Kafka was down"
attempt "$NAME-b" yes
