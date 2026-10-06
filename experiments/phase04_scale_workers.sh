#!/usr/bin/env bash
# Phase 04 experiment: 3 worker containers share 30 tasks via FOR UPDATE SKIP LOCKED.
#   experiments/phase04_scale_workers.sh
# Expected: every task COMPLETED once, work spread across the workers.
set -eu
cd "$(dirname "$0")/.."
psql() { docker compose exec -T postgres psql -U fileflow -c "$1"; }
TAG="exp4-$(date +%s)"

docker compose --profile app up -d --build --wait api >/dev/null
docker compose --profile app up -d --scale worker=3 worker >/dev/null
echo "workers: $(docker compose ps -q worker | wc -l)"

for i in $(seq 1 30); do
  printf 'file %s\nsome words here\n' "$i" | curl -sf -o /dev/null -F "file=@-;filename=$TAG-$i.txt" localhost:8000/files
done
echo "uploaded 30 files"

until [ "$(docker compose exec -T postgres psql -U fileflow -tAc "
  select count(*) from tasks t join files f on f.id = t.file_id
  where f.filename like '$TAG-%' and t.status in ('PENDING','PROCESSING')")" = "0" ]; do sleep 0.5; done

psql "select t.worker_id, t.status, count(*) as tasks
      from tasks t join files f on f.id = t.file_id
      where f.filename like '$TAG-%' group by 1, 2 order by 1"
psql "select count(*) as results from task_results r join tasks t on t.id = r.task_id
      join files f on f.id = t.file_id where f.filename like '$TAG-%'"
