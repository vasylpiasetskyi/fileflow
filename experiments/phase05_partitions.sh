#!/usr/bin/env bash
# Phase 05 experiment: partitions cap the parallelism of a consumer group.
#   experiments/phase05_partitions.sh
# Round 1: topic with 1 partition, 4 workers -> one worker does everything.
# Round 2: topic raised to 4 partitions      -> work spreads across workers.
# Partitions can never be reduced, so the script recreates the topic with 1
# partition first (dropping any events still in it — fine for dev).
set -eu
cd "$(dirname "$0")/.."
kt() { docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 "$@"; }
kg() { docker compose exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 "$@"; }
q()  { docker compose exec -T postgres psql -U fileflow -tAc "$1"; }

round() {  # $1 = tag
  for i in $(seq 1 20); do
    printf 'file %s\nsome words\n' "$i" | curl -sf -o /dev/null -F "file=@-;filename=$1-$i.txt" localhost:8000/files
  done
  until [ "$(q "select count(*) from tasks t join files f on f.id=t.file_id
                where f.filename like '$1-%' and t.status in ('PENDING','PROCESSING')")" = "0" ]; do sleep 0.5; done
  echo "--- tasks per worker:"
  q "select t.worker_id || ': ' || count(*) from tasks t join files f on f.id=t.file_id
     where f.filename like '$1-%' group by t.worker_id order by 1"
  echo "--- partition assignment:"
  kg --describe --group fileflow-workers 2>/dev/null | awk 'NR>1 && NF {print "  partition", $3, "lag", $6, "-> client", $9}'
  echo "--- group members (4 workers, but only members with partitions get work):"
  kg --describe --group fileflow-workers --members 2>/dev/null | awk 'NR>1 && NF {print "  ", $4, "partitions:", $5}'
}

docker compose --profile app stop worker >/dev/null 2>&1 || true
kt --delete --if-exists --topic file-processing
until ! kt --list | grep -qx file-processing; do sleep 0.5; done
kt --create --topic file-processing --partitions 1 --replication-factor 1 >/dev/null

docker compose --profile app up -d --build --wait api >/dev/null
docker compose --profile app up -d --scale worker=4 --force-recreate worker >/dev/null
sleep 8  # let the group finish its rebalance
echo "=== Round 1: $(kt --describe --topic file-processing | head -1 | grep -o 'PartitionCount: [0-9]*'), 4 workers"
round "exp5a-$(date +%s)"

kt --alter --topic file-processing --partitions 4
# BOTH sides cache topic metadata (refreshed every metadata.max.age.ms = 5 min):
# - the producer (in the API) would keep writing to partition 0 only;
# - the consumers would not rebalance onto the new partitions.
# Restarting both makes them pick up 4 partitions immediately.
docker compose --profile app up -d --force-recreate --wait api >/dev/null
docker compose --profile app up -d --scale worker=4 --force-recreate worker >/dev/null
sleep 8
echo "=== Round 2: $(kt --describe --topic file-processing | head -1 | grep -o 'PartitionCount: [0-9]*'), 4 workers"
round "exp5b-$(date +%s)"
