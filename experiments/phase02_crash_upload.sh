#!/usr/bin/env bash
# Phase 02 experiment: kill -9 the API while it streams a large file to MinIO.
#   experiments/phase02_crash_upload.sh
# Expected: row stays UPLOADING, no complete object, an incomplete multipart upload.
set -u
cd "$(dirname "$0")/.."
TMP=$(mktemp -d); trap 'rm -rf "$TMP"' EXIT
psql() { docker compose exec -T postgres psql -U fileflow -tAc "$1"; }
mcx()  { docker compose exec -T minio sh -c "mc alias set l http://localhost:9000 fileflow fileflow-secret >/dev/null; $1"; }
port_pid() { ss -ltnpH 'sport = :8000' | grep -o 'pid=[0-9]*' | cut -d= -f2 | head -1; }

head -c 500M /dev/urandom > "$TMP/big.bin"
NAME="crash-$(date +%s).bin"

old=$(port_pid); [ -n "$old" ] && kill -9 "$old"
setsid uv run uvicorn app.api.main:app --port 8000 > "$TMP/api.log" 2>&1 < /dev/null &
sleep 3
PID=$(port_pid); echo "api pid=$PID"

curl -s -o /dev/null -F "file=@$TMP/big.bin;filename=$NAME" localhost:8000/files &
until [ "$(psql "select count(*) from files where filename='$NAME' and status='UPLOADING'")" = "1" ]; do sleep 0.05; done
sleep 0.3
kill -9 "$PID" && echo "kill -9 $PID while streaming"
sleep 2

ID=$(psql "select id from files where filename='$NAME'")
echo "=== DB row:";             psql "select status, size from files where id='$ID'"
echo "=== complete object?";    mcx "mc stat l/fileflow/files/$ID 2>&1 | head -1"
echo "=== incomplete uploads:"; mcx "mc ls --incomplete l/fileflow/files/$ID"
