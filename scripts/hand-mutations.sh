#!/usr/bin/env bash
# Hand-made mutations: break one thing at a time, run that module's test suite, expect a failure.
# Complements PIT (which only mutates bsa-core) by also breaking Flink wiring, the sink, the API
# validation and the replayer. Each file is restored from a backup afterwards.
#
# Runs Maven directly, so call it inside the build container:
#   docker run --rm -v bsa-m2:/root/.m2 -v "$PWD":/src -w /src maven:3.9-eclipse-temurin-17 bash scripts/hand-mutations.sh
set -uo pipefail
cd "$(dirname "$0")/.."
OUT="${OUT:-results/hand-mutations.md}"
ONLY="${ONLY:-}"   # e.g. ONLY=M14,M20 re-runs a subset
# if interrupted, put every mutated file back
trap 'for b in $(find bsa-* -name "*.java.bak"); do mv "$b" "${b%.bak}"; touch "${b%.bak}"; done' EXIT

# id | module | file | perl substitution | what it breaks
MUTATIONS=(
"M01|bsa-core|bsa-core/src/main/java/bsa/core/rules/DataQualityGate.java|s/t\.eventTimeMs <= currentWatermark/t.eventTimeMs < currentWatermark/|sample exactly at the watermark treated as on time"
"M02|bsa-core|bsa-core/src/main/java/bsa/core/rules/DataQualityGate.java|s/if \(rangeViolation\(t, th\) != null\)/if (false)/|out-of-range samples accepted"
"M03|bsa-core|bsa-core/src/main/java/bsa/core/rules/WindowRules.java|s/slope < th\.sagVoltsPerMinute/slope <= th.sagVoltsPerMinute/|sag rule fires at exactly the threshold"
"M04|bsa-core|bsa-core/src/main/java/bsa/core/rules/WindowRules.java|s/s\.maxTemperatureC > th\.overTemperatureC/s.maxTemperatureC >= th.overTemperatureC/|over-temperature fires at exactly the limit"
"M05|bsa-core|bsa-core/src/main/java/bsa/core/rules/WindowStats.java|s/if \(t\.currentA < th\.loadCurrentA\)/if (true)/|resting samples enter the sag regression"
"M06|bsa-core|bsa-core/src/main/java/bsa/core/rules/CycleAnalyzer.java|s/0\.5 \* \(discharge\(prev\) \+ discharge\(t\)\)/discharge(t)/|capacity by rectangle instead of trapezoid rule"
"M07|bsa-core|bsa-core/src/main/java/bsa/core/rules/CycleAnalyzer.java|s/missing > 0 \|\| dt > th\.maxSampleGapMs/dt > th.maxSampleGapMs/|sequence holes no longer reported as gaps"
"M08|bsa-core|bsa-core/src/main/java/bsa/core/rules/FadeTracker.java|s/if \(!eolRaised && /if (/|end-of-life alert repeats every cycle"
"M09|bsa-core|bsa-core/src/main/java/bsa/core/rules/FadeTracker.java|s/        remember\(s\.capacityAh, th\);\n//|capacity history never updated"
"M10|bsa-core|bsa-core/src/main/java/bsa/core/model/Alert.java|s/String raw = cellId \+ \"\|\" \+ type\.name\(\) \+ \"\|\" \+ discriminator;/String raw = cellId + \"|\" + type.name();/|alert id ignores window\/cycle (idempotency key collision)"
"M11|bsa-flink|bsa-flink/src/main/java/bsa/flink/DataQualityFunction.java|s/                seen\.put\(key, t\.eventTimeMs\);\n//|dedup state never written: duplicates pass"
"M12|bsa-flink|bsa-flink/src/main/java/bsa/flink/TelemetryWatermarks.java|s/maxTs - bound - 1/maxTs - 1/g|watermark ignores the out-of-orderness bound"
"M13|bsa-flink|bsa-flink/src/main/java/bsa/flink/WindowFunctions.java|s/for \(Alert a : r\.gapAlerts\) ctx\.output\(GAP_ALERTS, a\);//|gap alerts never emitted"
"M14|bsa-flink|bsa-flink/src/main/java/bsa/flink/WindowFunctions.java|s/java\.util\.Collections\.sort\(due\);|Collections\.sort\(due\);/Collections.reverse(due);/|cycles fed to the fade tracker newest-first"
"M15|bsa-flink|bsa-flink/src/main/java/bsa/flink/sink/ClickHouseWriter.java|s/if \(resp\.statusCode\(\) >= 400 && resp\.statusCode\(\) < 500\) break;//|4xx (bad auth\/SQL) retried as if transient"
"M16|bsa-flink|bsa-flink/src/main/java/bsa/flink/sink/ClickHouseWriter.java|s/(public void flush\(boolean endOfInput\) throws IOException \{\n)        send\(\);/\$1/|checkpoint flush does nothing (breaks at-least-once)"
"M17|bsa-api|bsa-api/src/main/java/bsa/api/BatteryController.java|s/\@Max\(1000\) int limit\) \{\n        if \(!repo\.cellExists/\@Max(100000) int limit) {\n        if (!repo.cellExists/|cell alert limit no longer bounded"
"M18|bsa-api|bsa-api/src/main/java/bsa/api/BatteryController.java|s/static final String CELL_ID = \"\^\[A-Za-z0-9_-\]\{1,32\}\\$\";/static final String CELL_ID = \".*\";/|cell id whitelist removed"
"M19|bsa-api|bsa-api/src/main/java/bsa/api/ApiExceptionHandler.java|s/\"database unavailable\"/e.getMessage()/|database error text leaks to clients"
"M20|bsa-replayer|bsa-replayer/src/main/java/bsa/replayer/ReplayPlan.java|s/out\.sort\(Comparator\.comparingLong\(\(Delivery d\) -> d\.deliveryTimeMs\)\.thenComparingLong\(d -> d\.order\)\);//|delivery not re-sorted after delays (faults never shift)"
)

# README pins are checked by ReadmeNumbersTest; they are unrelated to the code under mutation.
TESTS='!ReadmeNumbersTest'

mvn -B -o -q clean >/dev/null 2>&1

# Baseline first: a mutation only counts as "caught" if the unmutated suite passes.
for module in bsa-core bsa-flink bsa-api bsa-replayer; do
  if ! mvn -B -o -q -pl "$module" -am test -Dtest="$TESTS" -Dsurefire.failIfNoSpecifiedTests=false >/dev/null 2>&1; then
    echo "baseline test run of $module FAILS - fix that first"; exit 1
  fi
  echo "baseline $module: green"
done

echo "# Hand mutations" > "$OUT"
echo >> "$OUT"
echo "Generated by \`scripts/hand-mutations.sh\` on $(date -u +%Y-%m-%dT%H:%MZ). Each row: one deliberate bug, the test suites of that module and the modules it depends on run against it (baseline run green first)." >> "$OUT"
echo >> "$OUT"
echo "| id | module | bug introduced | result | first failing test |" >> "$OUT"
echo "|---|---|---|---|---|" >> "$OUT"
caught=0; total=0
for m in "${MUTATIONS[@]}"; do
  IFS='|' read -r id module file subst what <<< "$m"
  # the perl substitution itself may contain '|' in alternations; re-split from the end instead
  what="${m##*|}"; rest="${m%|*}"; subst="${rest#*|*|*|}"
  if [ -n "$ONLY" ] && [[ ",$ONLY," != *",$id,"* ]]; then continue; fi
  cp "$file" "$file.bak"
  perl -0pi -e "$subst" "$file"
  if cmp -s "$file" "$file.bak"; then
    echo "| $id | $module | $what | NOT APPLIED (pattern not found) | |" >> "$OUT"
    echo "$id NOT APPLIED"; rm -f "$file.bak"; touch "$file"; continue
  fi
  total=$((total + 1))
  log=$(mvn -B -o -q -pl "$module" -am test -Dtest="$TESTS" -Dsurefire.failIfNoSpecifiedTests=false 2>&1)
  rc=$?
  mv "$file.bak" "$file"
  # mv keeps the backup's old mtime; without touch, Maven's incremental compiler would keep the
  # mutated .class and contaminate every later run (this happened on the first attempt).
  touch "$file"
  if [ $rc -ne 0 ]; then
    caught=$((caught + 1))
    first=$(echo "$log" | grep -E '^\[ERROR\] +[A-Za-z]+Test\.' | head -1 | grep -oE '[A-Za-z]+Test\.[A-Za-z0-9_]+' || true)
    [ -z "$first" ] && first=$(echo "$log" | grep -m1 -oE 'COMPILATION ERROR|[A-Za-z]+Test\.[A-Za-z0-9_]+' || echo "other failure")
    echo "| $id | $module | $what | caught | \`$first\` |" >> "$OUT"
    echo "$id caught by $first"
  else
    echo "| $id | $module | $what | **NOT caught** | |" >> "$OUT"
    echo "$id NOT CAUGHT"
  fi
done
echo >> "$OUT"
echo "**$caught of $total applied mutations caught.**" >> "$OUT"
echo "caught $caught / $total"
