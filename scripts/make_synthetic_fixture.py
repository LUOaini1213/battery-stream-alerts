#!/usr/bin/env python3
"""Generate the tiny synthetic fixture used by the tests (no real data involved).

Two cells x 8 discharge cycles x 45 samples, 10 s apart, cycles 1 h apart.
  * samples 36-44 of every cycle form a steep knee (-0.48 V/min) -> one VOLTAGE_SAG per cycle (16)
  * SYN-B cycles 6-8 run hotter (max 44.8 C) -> OVER_TEMPERATURE in 2 windows per cycle (6)
  * SYN-A cycle 6 is discharged at 1.8 A instead of 2.0 A -> CAPACITY_DROP (1)
  * SYN-B current steps down 2.0 ... 1.5 A -> CAPACITY_DROP at cycles 6-8 (3) + CAPACITY_EOL at cycle 8 (1);
    cycle 7 uses 1.62 A rather than 1.6 A so that its SOH (0.81) is not on the 0.80 line,
    where floating-point rounding would decide the outcome
Output is sorted by (event_time_ms, cell_id) like the real converter output.
"""
import csv
import sys
from pathlib import Path

T0 = 1767225600000  # 2026-01-01T00:00:00Z, a multiple of the 120 s window
CURRENT = {
    "SYN-A": [2.0, 2.0, 2.0, 2.0, 2.0, 1.8, 2.0, 2.0],
    "SYN-B": [2.0, 2.0, 2.0, 1.9, 1.8, 1.7, 1.62, 1.5],
}


def main(out: Path) -> None:
    rows = []
    for cell, currents in CURRENT.items():
        for c, amps in enumerate(currents, start=1):
            start = T0 + (c - 1) * 3_600_000
            hot = cell == "SYN-B" and c >= 6
            for k in range(45):
                v = 4.1 - 0.004 * min(k, 35) - (0.08 * (k - 35) if k > 35 else 0.0)
                i = 0.0 if k == 0 else -amps
                temp = 25.0 + (0.45 if hot else 0.3) * k
                rows.append((start + k * 10_000, cell, c, k, v, i, temp))
    rows.sort(key=lambda r: (r[0], r[1]))
    out.parent.mkdir(parents=True, exist_ok=True)
    with open(out, "w", newline="") as f:
        w = csv.writer(f, lineterminator="\n")
        w.writerow(["event_time_ms", "cell_id", "cycle", "seq", "voltage_v", "current_a", "temperature_c"])
        for r in rows:
            w.writerow([r[0], r[1], r[2], r[3], f"{r[4]:.6f}", f"{r[5]:.6f}", f"{r[6]:.4f}"])
    print(f"wrote {len(rows)} rows to {out}")


if __name__ == "__main__":
    main(Path(sys.argv[1]) if len(sys.argv) > 1 else Path(__file__).resolve().parent.parent / "fixtures" / "synthetic_telemetry.csv")
