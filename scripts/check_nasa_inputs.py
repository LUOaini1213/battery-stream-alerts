#!/usr/bin/env python3
"""Check a local copy of the NASA battery files against results/nasa-inputs.json (stdlib only).

    python scripts/check_nasa_inputs.py --raw-dir /path/to/mat/files [--data-dir ./data]

--raw-dir   folder holding B0005.mat, B0006.mat, B0007.mat, B0018.mat: SHA-256 of each must match.
--data-dir  optional output folder of scripts/convert_nasa.py: convert_summary.json must report the
            same sample and cycle totals (the CSV hashes are informational, since they were produced
            with the numpy/scipy versions named in the JSON).
Exit code 0 when everything matches, 1 otherwise.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import sys
from pathlib import Path

EXPECTED = Path(__file__).resolve().parent.parent / "results" / "nasa-inputs.json"


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--raw-dir", required=True, type=Path)
    ap.add_argument("--data-dir", type=Path)
    a = ap.parse_args()
    exp = json.loads(EXPECTED.read_text(encoding="utf-8"))
    bad = 0
    for cell, want in exp["mat_sha256"].items():
        p = a.raw_dir / f"{cell}.mat"
        if not p.is_file():
            print(f"MISSING {p}")
            bad += 1
            continue
        got = sha256(p)
        print(("OK      " if got == want else "MISMATCH") + f" {cell}.mat {got}")
        bad += got != want
    if a.data_dir is not None:
        conv = exp["converter"]
        summ = json.loads((a.data_dir / "convert_summary.json").read_text(encoding="utf-8"))
        got_cycles = sum(c["discharge_cycles"] for c in summ["cells"].values())
        for name, got, want in [("total_samples", summ["total_samples"], conv["total_samples"]),
                                ("discharge_cycles", got_cycles, conv["discharge_cycles"])]:
            print(("OK      " if got == want else "MISMATCH") + f" {name} {got} (expected {want})")
            bad += got != want
        for csv_name, key in [("telemetry.csv", "telemetry_csv_sha256"),
                              ("reference_capacity.csv", "reference_capacity_csv_sha256")]:
            p = a.data_dir / csv_name
            if p.is_file():
                same = sha256(p) == conv[key]
                print(f"INFO    {csv_name} sha256 {'matches' if same else 'differs from'} the reference conversion")
    print("ALL NASA INPUT CHECKS PASSED" if bad == 0 else f"{bad} NASA INPUT CHECK(S) FAILED")
    return 0 if bad == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
