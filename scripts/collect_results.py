#!/usr/bin/env python3
"""Collect end-to-end numbers from ClickHouse into results/e2e-<mode>.json (stdlib only).

Definitions (all wall-clock, same host clock for every container):
  ingest_to_stored_ms   ClickHouse inserted_at - replayer send time, per accepted telemetry row
  alert_emit_ms         Flink emit time - latest replayer send time among the alert's inputs
  alert_stored_ms       ClickHouse inserted_at - the same latest send time
Window and cycle alerts can only fire after the watermark passes the end of their window, so their
latency includes waiting for later data; the replay speed-up decides how long that is in wall time.
"""
from __future__ import annotations

import argparse
import base64
import csv
import json
import urllib.request
from pathlib import Path

DQ = ["DQ_DUPLICATE", "DQ_LATE", "DQ_OUT_OF_RANGE", "DQ_GAP"]
FAMILIES = {
    "window (OVER_TEMPERATURE, VOLTAGE_SAG)": ["OVER_TEMPERATURE", "VOLTAGE_SAG"],
    "cycle (CAPACITY_DROP, CAPACITY_EOL, DQ_GAP)": ["CAPACITY_DROP", "CAPACITY_EOL", "DQ_GAP"],
    "per-sample (DQ_LATE, DQ_DUPLICATE, DQ_OUT_OF_RANGE)": ["DQ_LATE", "DQ_DUPLICATE", "DQ_OUT_OF_RANGE"],
}


class CH:
    def __init__(self, url: str, user: str, password: str):
        self.url = url.rstrip("/") + "/"
        self.auth = "Basic " + base64.b64encode(f"{user}:{password}".encode()).decode()

    def rows(self, sql: str) -> list[dict]:
        req = urllib.request.Request(self.url, data=(sql + " FORMAT JSON").encode(), headers={"Authorization": self.auth})
        with urllib.request.urlopen(req, timeout=60) as r:
            return json.loads(r.read())["data"]

    def one(self, sql: str):
        return list(self.rows(sql)[0].values())[0]


def quantiles(ch: CH, expr: str, table: str, where: str = "1") -> dict:
    r = ch.rows(f"SELECT count() AS n, quantilesExact(0.5, 0.95, 0.99)({expr}) AS q, max({expr}) AS mx "
                f"FROM {table} FINAL WHERE {where}")[0]
    if int(r["n"]) == 0:
        return {"n": 0}
    return {"n": int(r["n"]), "p50": int(r["q"][0]), "p95": int(r["q"][1]), "p99": int(r["q"][2]), "max": int(r["mx"])}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", required=True)
    ap.add_argument("--ch", default="http://localhost:18123")
    ap.add_argument("--user", default="bsa_admin")
    ap.add_argument("--password", required=True)
    ap.add_argument("--replay-report", required=True)
    ap.add_argument("--reference", required=True)
    ap.add_argument("--speedup", type=float, required=True)
    ap.add_argument("--out", required=True)
    a = ap.parse_args()
    ch = CH(a.ch, a.user, a.password)
    replay = json.loads(Path(a.replay_report).read_text())

    out: dict = {"mode": a.mode, "speedup": a.speedup}
    out["replay"] = {k: replay[k] for k in ["csv_rows", "sample_records_sent", "heartbeat_records_sent", "wall_ms",
                                            "records_per_s", "partitions", "injected", "expected_dq_alerts"]}
    out["rows"] = {
        t: {"raw": int(ch.one(f"SELECT count() FROM bsa.{t}")), "final": int(ch.one(f"SELECT count() FROM bsa.{t} FINAL"))}
        for t in ["telemetry", "cycles", "alerts"]
    }
    # raw count() is taken after background merges may already have collapsed duplicates, so also
    # report how many rows the sink actually inserted (from ClickHouse's own query log)
    try:
        urllib.request.urlopen(urllib.request.Request(ch.url, data=b"SYSTEM FLUSH LOGS", headers={"Authorization": ch.auth}), timeout=60).read()
        for t in ["telemetry", "cycles", "alerts"]:
            out["rows"][t]["inserted"] = int(ch.one(
                "SELECT sum(written_rows) FROM system.query_log WHERE type = 'QueryFinish' "
                f"AND query_kind = 'Insert' AND has(tables, 'bsa.{t}')"))
    except Exception as e:  # query_log may be disabled on other setups
        out["rows"]["inserted_error"] = str(e)
    by_type = {r["alert_type"]: int(r["n"]) for r in ch.rows(
        "SELECT alert_type, count() AS n FROM bsa.alerts FINAL GROUP BY alert_type ORDER BY alert_type")}
    out["alerts_by_type"] = by_type
    out["alerts_by_cell_and_type"] = {}
    for r in ch.rows("SELECT cell_id, alert_type, count() AS n FROM bsa.alerts FINAL GROUP BY cell_id, alert_type ORDER BY cell_id, alert_type"):
        out["alerts_by_cell_and_type"].setdefault(r["cell_id"], {})[r["alert_type"]] = int(r["n"])
    if a.mode != "reprocess":  # a second pass re-uses old send times, so latency is meaningless there
        out["latency_ms"] = {
            "ingest_to_stored (telemetry)": quantiles(ch, "toUnixTimestamp64Milli(inserted_at) - ingest_ms", "bsa.telemetry"),
        }
        for name, types in FAMILIES.items():
            tl = ",".join(f"'{t}'" for t in types)
            out["latency_ms"][f"alert_emit {name}"] = quantiles(ch, "emitted_ms - max_ingest_ms", "bsa.alerts", f"alert_type IN ({tl})")
            out["latency_ms"][f"alert_stored {name}"] = quantiles(
                ch, "toUnixTimestamp64Milli(inserted_at) - max_ingest_ms", "bsa.alerts", f"alert_type IN ({tl})")

    # streaming capacity vs the capacity NASA reports for each discharge cycle
    ref = {}
    with open(a.reference) as f:
        for r in csv.DictReader(f):
            ref[(r["cell_id"], int(r["cycle"]))] = float(r["capacity_ah"])
    cyc = ch.rows("SELECT cell_id, cycle, capacity_ah, soh, samples, missing_samples FROM bsa.cycles FINAL ORDER BY cell_id, cycle")
    errs = [float(c["capacity_ah"]) - ref[(c["cell_id"], int(c["cycle"]))] for c in cyc if (c["cell_id"], int(c["cycle"])) in ref]
    rel = [abs(float(c["capacity_ah"]) / ref[(c["cell_id"], int(c["cycle"]))] - 1) for c in cyc if (c["cell_id"], int(c["cycle"])) in ref]
    out["capacity_vs_nasa"] = {
        "cycles_matched": len(errs),
        "cycles_in_reference": len(ref),
        "mean_abs_error_ah": round(sum(map(abs, errs)) / len(errs), 5) if errs else None,
        "max_abs_error_ah": round(max(map(abs, errs)), 5) if errs else None,
        "mean_signed_error_ah": round(sum(errs) / len(errs), 5) if errs else None,
        "mean_abs_relative_error_pct": round(100 * sum(rel) / len(rel), 3) if rel else None,
    }
    # end-of-life cycle: streaming alert vs the same rule applied to NASA's own capacities
    eol_stream = {r["cell_id"]: int(r["cycle"]) for r in ch.rows(
        "SELECT cell_id, min(cycle) AS cycle FROM bsa.alerts FINAL WHERE alert_type = 'CAPACITY_EOL' GROUP BY cell_id")}
    eol = {}
    for cell in sorted({k[0] for k in ref}):
        caps = [ref[(cell, n)] for n in sorted(n for (c, n) in ref if c == cell)]
        init = sum(caps[:3]) / 3
        first = next((i + 1 for i, x in enumerate(caps) if i >= 3 and x / init < 0.8), None)
        eol[cell] = {"stream_eol_cycle": eol_stream.get(cell), "nasa_capacity_eol_cycle": first,
                     "nasa_initial_ah": round(init, 4), "nasa_last_ah": round(caps[-1], 4)}
    out["end_of_life"] = eol

    if replay.get("expected_dq_alerts") and any(replay["expected_dq_alerts"].values()):
        out["dq_expected_vs_detected"] = {t: {"expected": replay["expected_dq_alerts"].get(t, 0), "detected": by_type.get(t, 0)} for t in DQ}

    ids = sorted(f'{r["alert_type"]}|{r["cell_id"]}|{r["id"]}' for r in ch.rows(
        "SELECT alert_type, cell_id, toString(alert_id) AS id FROM bsa.alerts FINAL"))
    run_dir = Path(a.out).parent / "run"
    run_dir.mkdir(parents=True, exist_ok=True)
    (run_dir / f"alert-ids-{a.mode}.txt").write_text("\n".join(ids) + "\n")
    clean_ids = run_dir / "alert-ids-clean.txt"
    if a.mode != "clean" and clean_ids.exists():
        base = set(clean_ids.read_text().split())
        cur = set(ids)
        non_dq = lambda s: {x for x in s if not x.startswith("DQ_")}
        out["vs_clean_run"] = {
            "identical_alert_set": cur == base,
            "only_in_this_run": len(cur - base),
            "only_in_clean_run": len(base - cur),
            "non_dq_only_in_this_run": len(non_dq(cur) - non_dq(base)),
            "non_dq_only_in_clean_run": len(non_dq(base) - non_dq(cur)),
        }

    Path(a.out).write_text(json.dumps(out, indent=2) + "\n")
    print(json.dumps(out, indent=2))


if __name__ == "__main__":
    main()
