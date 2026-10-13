#!/usr/bin/env python3
"""Convert NASA PCoE battery .mat files into a compact per-sample discharge CSV.

Source data: B. Saha and K. Goebel (2007). "Battery Data Set", NASA Prognostics
Data Repository, NASA Ames Research Center, Moffett Field, CA.
The NASA catalog does not state a licence, so the raw files and the converted
CSV are NOT committed to this repository. Point --raw-dir at your own copy.

Output (written to --out-dir, default ./data, which is git-ignored):
  telemetry.csv           one row per discharge sample, sorted by (event_time_ms, cell_id, cycle, seq)
  reference_capacity.csv  NASA-provided capacity per discharge cycle (for cross-checking the stream job)
  convert_summary.json    row counts, time span, per-cell stats

Event time: each cycle carries a MATLAB date vector [Y M D h m s] for its start.
The repository does not state a time zone; we interpret it as UTC so that the
replay is deterministic. Sample event time = cycle start + Time[s].
"""
from __future__ import annotations

import argparse
import csv
import json
import math
from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import scipy.io as sio

CELLS = ["B0005", "B0006", "B0007", "B0018"]


def datevec_to_ms(v) -> int:
    v = np.ravel(v)
    y, mo, d, h, mi = (int(x) for x in v[:5])
    s = float(v[5])
    base = datetime(y, mo, d, h, mi, tzinfo=timezone.utc)
    return int(round((base + timedelta(seconds=s)).timestamp() * 1000))


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--raw-dir", required=True)
    ap.add_argument("--out-dir", default="data")
    args = ap.parse_args()
    raw, out = Path(args.raw_dir), Path(args.out_dir)
    out.mkdir(parents=True, exist_ok=True)

    rows = []
    ref = []
    summary = {"source": "Saha & Goebel 2007, NASA PCoE Battery Data Set", "cells": {}}
    for cell in CELLS:
        m = sio.loadmat(raw / f"{cell}.mat", simplify_cells=True)
        cycles = m[cell]["cycle"]
        k = 0
        n_cell = 0
        nan_rows = 0
        for c in cycles:
            if c["type"] != "discharge":
                continue
            k += 1
            start = datevec_to_ms(c["time"])
            d = c["data"]
            t = np.ravel(d["Time"]).astype(float)
            v = np.ravel(d["Voltage_measured"]).astype(float)
            i = np.ravel(d["Current_measured"]).astype(float)
            temp = np.ravel(d["Temperature_measured"]).astype(float)
            cap = np.ravel(d["Capacity"])
            ref.append((cell, k, float(cap[0]) if cap.size else float("nan"), start))
            for j in range(len(t)):
                vals = (v[j], i[j], temp[j])
                if any(math.isnan(x) for x in vals):
                    nan_rows += 1
                rows.append((start + int(round(t[j] * 1000)), cell, k, j, v[j], i[j], temp[j]))
                n_cell += 1
        summary["cells"][cell] = {"discharge_cycles": k, "samples": n_cell, "rows_with_nan": nan_rows}

    rows.sort(key=lambda r: (r[0], r[1], r[2], r[3]))
    with open(out / "telemetry.csv", "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["event_time_ms", "cell_id", "cycle", "seq", "voltage_v", "current_a", "temperature_c"])
        for r in rows:
            w.writerow([r[0], r[1], r[2], r[3], f"{r[4]:.6f}", f"{r[5]:.6f}", f"{r[6]:.4f}"])
    with open(out / "reference_capacity.csv", "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["cell_id", "cycle", "capacity_ah", "cycle_start_ms"])
        for r in ref:
            w.writerow([r[0], r[1], f"{r[2]:.6f}", r[3]])

    # non-monotonic event time within a cell (overlapping cycles) would break the replay story
    by_cell: dict[str, list[int]] = {}
    for r in sorted(rows, key=lambda r: (r[1], r[2], r[3])):
        by_cell.setdefault(r[1], []).append(r[0])
    for cell, ts in by_cell.items():
        dts = np.diff(np.asarray(ts))
        summary["cells"][cell].update(
            {
                "non_increasing_steps": int((dts <= 0).sum()),
                "first_event": datetime.fromtimestamp(ts[0] / 1000, timezone.utc).isoformat(),
                "last_event": datetime.fromtimestamp(ts[-1] / 1000, timezone.utc).isoformat(),
            }
        )
    arr = np.asarray([[r[4], r[5], r[6]] for r in rows])
    summary["total_samples"] = len(rows)
    summary["voltage_v"] = [float(np.nanmin(arr[:, 0])), float(np.nanmax(arr[:, 0]))]
    summary["current_a"] = [float(np.nanmin(arr[:, 1])), float(np.nanmax(arr[:, 1]))]
    summary["temperature_c"] = [float(np.nanmin(arr[:, 2])), float(np.nanmax(arr[:, 2]))]
    (out / "convert_summary.json").write_text(json.dumps(summary, indent=2))
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
