#!/usr/bin/env python3
"""
Summarize integrity-guard events.

Input:
  - ckpt/events.csv  (columns: ts, ok, verify_ms, rollback_ms)

Output (default):
  - trace/events_summary.txt  (one line with KPIs)

"""

import argparse
import csv
import os
from typing import List, Optional

def to_float(x: Optional[str]) -> Optional[float]:
    if x is None or x == "":
        return None
    try:
        return float(x)
    except ValueError:
        return None

def avg(xs: List[Optional[float]]) -> float:
    vals = [v for v in xs if v is not None]
    return sum(vals) / len(vals) if vals else 0.0

def main() -> None:
    p = argparse.ArgumentParser(description="Summarize events.csv into a one-line KPI.")
    p.add_argument("--events", default="ckpt/events.csv", help="path to events.csv")
    p.add_argument("--out", default="trace/events_summary.txt", help="output path")
    args = p.parse_args()

    if not os.path.exists(args.events):
        raise SystemExit(f"events not found: {args.events}")

    with open(args.events, newline="") as f:
        rows = list(csv.DictReader(f))

    n_total = len(rows)
    detected = [r for r in rows if r.get("ok") == "False"]

    # parse metrics
    v_all = [to_float(r.get("verify_ms")) for r in rows]
    v_det = [to_float(r.get("verify_ms")) for r in detected]
    r_det = [to_float(r.get("rollback_ms")) for r in detected]

    # ensure parent dir exists
    out_dir = os.path.dirname(args.out) or "."
    os.makedirs(out_dir, exist_ok=True)

    line = (
        f"events={n_total}, "
        f"detected={len(detected)}, "
        f"verify_ms_avg_all={avg(v_all):.1f}, "
        f"verify_ms_avg_detected={avg(v_det):.1f}, "
        f"rollback_ms_avg_detected={avg(r_det):.1f}\n"
    )

    with open(args.out, "w") as w:
        w.write(line)

    # also print to console for convenience
    print(line.strip())

if __name__ == "__main__":
    main()
