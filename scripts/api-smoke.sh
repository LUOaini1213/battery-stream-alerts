#!/usr/bin/env bash
# Black-box checks of the running API and of the database permissions. Exit code != 0 on any failure.
set -uo pipefail
API="${API:-http://localhost:${BSA_API_PORT:-18080}}"
CH_URL="${CH_URL:-http://localhost:18123}"
READER_PW="${BSA_READER_PASSWORD:-local-reader-change-me}"
fail=0

check() { # name expected_status url
  local code
  code=$(curl -s -o /tmp/bsa-smoke-body -w '%{http_code}' "$3")
  if [ "$code" = "$2" ]; then echo "PASS $1 ($code)"; else echo "FAIL $1: expected $2 got $code: $(head -c 300 /tmp/bsa-smoke-body)"; fail=1; fi
}

check "health is UP"                 200 "$API/actuator/health"
grep -q '"status":"UP"' /tmp/bsa-smoke-body || { echo "FAIL health body"; fail=1; }
check "list cells"                   200 "$API/api/v1/cells"
echo "     cells: $(grep -o '"cellId":"[^"]*"' /tmp/bsa-smoke-body | tr '\n' ' ')"
check "cell detail"                  200 "$API/api/v1/cells/B0005"
check "cell alerts filtered"         200 "$API/api/v1/cells/B0005/alerts?type=CAPACITY_EOL&limit=5"
echo "     B0005 EOL: $(head -c 400 /tmp/bsa-smoke-body)"
check "cell cycles"                  200 "$API/api/v1/cells/B0006/cycles?limit=3"
check "alerts since"                 200 "$API/api/v1/alerts?since=2008-04-02T00:00:00Z&until=2008-04-03T00:00:00Z&limit=3"
check "alert summary"                200 "$API/api/v1/alerts/summary"
echo "     summary: $(cat /tmp/bsa-smoke-body)"
check "openapi document"             200 "$API/v3/api-docs"
check "unknown cell -> 404"          404 "$API/api/v1/cells/B9999/alerts"
check "injection attempt -> 400"     400 "$API/api/v1/cells/B0005%27%20OR%201%3D1%20--/alerts"
check "limit too large -> 400"       400 "$API/api/v1/cells/B0005/alerts?limit=5000"
check "unknown type -> 400"          400 "$API/api/v1/alerts?since=2008-04-02T00:00:00Z&type=DROP_TABLE"
check "missing since -> 400"         400 "$API/api/v1/alerts"
check "bad since -> 400"             400 "$API/api/v1/alerts?since=yesterday"

# the API's database user must not be able to write
code=$(curl -s -o /tmp/bsa-smoke-body -w '%{http_code}' -u "bsa_reader:$READER_PW" "$CH_URL/" \
  --data-binary "INSERT INTO bsa.alerts (alert_id, cell_id, alert_type, severity, event_time_ms, window_start_ms, window_end_ms, cycle, detail, max_ingest_ms, emitted_ms) VALUES (generateUUIDv4(), 'X', 'X', 'X', 0, 0, 0, 0, '', 0, 0)")
if [ "$code" != "200" ]; then echo "PASS reader cannot INSERT ($code: $(grep -o 'Code: [0-9]*' /tmp/bsa-smoke-body | head -1))"; else echo "FAIL reader could INSERT"; fail=1; fi
rm -f /tmp/bsa-smoke-body
[ "$fail" = 0 ] && echo "ALL API SMOKE CHECKS PASSED"
exit "$fail"
