#!/usr/bin/env bash
# End-to-end run: fresh stack -> replay the converted NASA CSV -> wait until the job is idle ->
# collect counts/latency into results/ -> smoke-test the API.
#
#   scripts/demo.sh clean       replay as recorded
#   scripts/demo.sh faults      replay with injected duplicates/reordering/late/out-of-range/dropped samples
#   scripts/demo.sh crash       replay; SIGKILL the Flink container mid-way; restart it from the retained checkpoint
#   scripts/demo.sh reprocess   replay; then re-run the job from offset 0 with empty state (every row written twice)
#
# Env: BSA_DATA_DIR (folder with telemetry.csv + reference_capacity.csv), SPEEDUP (default 5000),
#      BSA_DOCKERFILE (Dockerfile = build from source; Dockerfile.prebuilt = use jars from target/).
set -euo pipefail
cd "$(dirname "$0")/.."

MODE="${1:-clean}"
case "$MODE" in clean|faults|crash|reprocess) ;; *) echo "unknown mode $MODE"; exit 2 ;; esac
export BSA_DATA_DIR="${BSA_DATA_DIR:-$PWD/data}"
export BSA_DOCKERFILE="${BSA_DOCKERFILE:-Dockerfile}"
SPEEDUP="${SPEEDUP:-5000}"
FAULTS="${FAULTS:-dup=40,reorder=40,late=20,oor=20,drop=20}"
ADMIN_PW="${BSA_ADMIN_PASSWORD:-local-admin-change-me}"
READER_PW="${BSA_READER_PASSWORD:-local-reader-change-me}"
CH_URL="http://localhost:18123"

[ -f "$BSA_DATA_DIR/telemetry.csv" ] || { echo "missing $BSA_DATA_DIR/telemetry.csv - run scripts/convert_nasa.py first"; exit 1; }
mkdir -p results/run
chmod 777 results/run 2>/dev/null || true

ch() { curl -fsS -u "bsa_admin:$ADMIN_PW" "$CH_URL/" --data-binary "$1"; }

wait_idle() { # until row counts stop changing for 4 polls in a row
  local prev="" same=0 cur
  for _ in $(seq 1 240); do
    sleep 5
    cur=$(ch "SELECT (SELECT count() FROM bsa.telemetry), (SELECT count() FROM bsa.alerts), (SELECT count() FROM bsa.cycles) FORMAT TSV" 2>/dev/null || echo "?")
    echo "  rows telemetry/alerts/cycles: $cur"
    if [ "$cur" = "$prev" ] && [ "$cur" != "?" ]; then same=$((same + 1)); else same=0; fi
    prev="$cur"
    [ "$same" -ge 4 ] && return 0
  done
  echo "timed out waiting for the job to go idle"; return 1
}

echo "== [$MODE] fresh stack (down -v, build, up)"
docker compose down -v --remove-orphans >/dev/null 2>&1 || true
docker compose --profile replay build
docker compose up -d --wait redpanda clickhouse api
docker compose up -d flink-job   # starts after redpanda-init created the topic

REPLAY=(--csv /data/telemetry.csv --bootstrap redpanda:9092 --topic bms.telemetry --partitions 4
        --speedup "$SPEEDUP" --max-sleep-ms 20 --heartbeat-ms 60000 --seed 42 --report "/results/replay-$MODE.json")
[ "$MODE" = faults ] && REPLAY+=(--faults "$FAULTS")

if [ "$MODE" = crash ]; then
  echo "== replay in background; SIGKILL the Flink JVM part-way through"
  docker compose run -d --rm --name bsa-replayer-run replayer "${REPLAY[@]}" >/dev/null
  until [ "$(ch 'SELECT count() FROM bsa.telemetry FORMAT TSV' || echo 0)" -gt 60000 ]; do sleep 1; done
  echo "  telemetry rows before kill: $(ch 'SELECT count() FROM bsa.telemetry FORMAT TSV')"
  docker kill bsa-flink-job >/dev/null
  echo "  killed bsa-flink-job; restarting in 10 s (it restores the newest retained checkpoint)"
  sleep 10
  docker compose start flink-job >/dev/null
  docker wait bsa-replayer-run >/dev/null 2>&1 || true
  docker logs bsa-flink-job 2>&1 | grep -E "Restoring from retained checkpoint|No retained checkpoint" | tail -1 | tee "results/run/crash-restore.log"
else
  echo "== replay"
  docker compose run --rm replayer "${REPLAY[@]}" | tail -25
fi

echo "== waiting for the job to drain"
wait_idle

if [ "$MODE" = reprocess ]; then
  echo "== re-run the job from offset 0 with empty state: every row is written a second time"
  docker compose stop flink-job >/dev/null
  KAFKA_GROUP=bsa-alert-job-reprocess RESUME_FROM_CHECKPOINT=false docker compose up -d --no-deps --force-recreate flink-job
  wait_idle
fi

python3 scripts/collect_results.py --mode "$MODE" --ch "$CH_URL" --user bsa_admin --password "$ADMIN_PW" \
  --replay-report "results/run/replay-$MODE.json" --reference "$BSA_DATA_DIR/reference_capacity.csv" \
  --speedup "$SPEEDUP" --out "results/e2e-$MODE.json"

BSA_READER_PASSWORD="$READER_PW" bash scripts/api-smoke.sh | tee "results/run/api-smoke-$MODE.txt"
echo "== done ($MODE). Stop with: docker compose down -v"
