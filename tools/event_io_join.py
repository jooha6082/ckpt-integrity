#!/usr/bin/env python3
"""
event_io_join.py

Join detection events with iostat throughput around each event.

This version automatically detects when no time column is present
(e.g., your iostat.csv has only a 'sample' index) and falls back to
index-based averaging.

Input:
  --events  ckpt/events.csv   (needs 'ts' and 'ok' columns)
  --iostat  trace/iostat.csv  (requires throughput column, e.g. 'mb_s')

Output:
  trace/event_io.csv   (columns: event_ts, io_avg_MBps_pmW, mode)

Typical usage:
  python tools/event_io_join.py \
      --events ckpt/events.csv \
      --iostat trace/iostat.csv \
      --window 5 \
      --out trace/event_io.csv
"""

import argparse
import bisect
import csv
import math
import os
from datetime import datetime
from typing import List, Optional, Tuple

# Common column name candidates
TIME_KEYS = ["ts", "timestamp", "time", "datetime"]
MB_KEYS = ["MB_s", "MB/s", "mb_s", "MB_per_s", "MBps"]

def to_float(x: Optional[str]) -> float:
    """Safe float parser (returns NaN on failure)."""
    try:
        return float(x) if x not in (None, "") else math.nan
    except Exception:
        return math.nan

def parse_time_value(x: str) -> Optional[float]:
    """Try to parse as epoch seconds or human-readable time."""
    try:
        v = float(x)
        if v > 1e6:  # looks like UNIX epoch seconds
            return v
        return None
    except Exception:
        pass
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y/%m/%d %H:%M:%S"):
        try:
            return datetime.strptime(x, fmt).timestamp()
        except Exception:
            pass
    return None

def pick_key(header: List[str], candidates: List[str]) -> Optional[str]:
    """Return the first header name that matches a candidate list."""
    for c in candidates:
        if c in header:
            return c
    return None

def moving_avg_by_time(ts: List[float], mb: List[float], center: float, half: float) -> float:
    lo = center - half
    hi = center + half
    i = bisect.bisect_left(ts, lo)
    j = bisect.bisect_right(ts, hi)
    if i >= j:
        return math.nan
    vals = [mb[k] for k in range(i, j) if not math.isnan(mb[k])]
    return sum(vals) / len(vals) if vals else math.nan

def moving_avg_by_index(mb: List[float], center_idx: int, halfwin: int) -> float:
    """Average ±halfwin samples around a center index."""
    i = max(0, center_idx - halfwin)
    j = min(len(mb), center_idx + halfwin + 1)
    vals = [v for v in mb[i:j] if not math.isnan(v)]
    return sum(vals) / len(vals) if vals else math.nan

def main() -> None:
    p = argparse.ArgumentParser(description="Correlate detected faults with I/O activity.")
    p.add_argument("--events", default="ckpt/events.csv", help="Path to events.csv")
    p.add_argument("--iostat", default="trace/iostat.csv", help="Path to iostat.csv")
    p.add_argument("--window", type=float, default=5.0,
                   help="Half window size (seconds or samples)")
    p.add_argument("--out", default="trace/event_io.csv", help="Output CSV path")
    args = p.parse_args()

    # Load events (only those with ok == False)
    if not os.path.exists(args.events):
        raise SystemExit(f"events not found: {args.events}")
    det_ts: List[float] = []
    with open(args.events, newline="") as f:
        for row in csv.DictReader(f):
            if row.get("ok") == "False":
                try:
                    det_ts.append(float(row["ts"]))
                except Exception:
                    pass
    det_ts.sort()

    # Load iostat
    if not os.path.exists(args.iostat):
        raise SystemExit(f"iostat not found: {args.iostat}")
    with open(args.iostat, newline="") as f:
        irows = list(csv.DictReader(f))
    if not irows:
        raise SystemExit("iostat.csv is empty")

    header = list(irows[0].keys())

    # Detect columns
    tkey = pick_key(header, TIME_KEYS)
    mkey = pick_key(header, MB_KEYS)
    if not mkey:
        raise SystemExit(f"Could not find MB/s column. Tried: {MB_KEYS}")

    # Parse MB/s values
    mb = [to_float(r.get(mkey)) for r in irows]

    # Parse time if available, else fall back to index mode
    ts_raw: List[Optional[float]] = []
    epoch_like = False
    if tkey:
        ts_raw = [parse_time_value(r.get(tkey, "")) for r in irows]
        epoch_like = all((isinstance(v, float) and v > 1e6) for v in ts_raw if v is not None)

    if not tkey:
        # No time column (like your iostat.csv with only 'sample')
        ts = list(range(len(mb)))
        mode = "index"
    elif epoch_like:
        pairs: List[Tuple[float, float]] = sorted(
            (ts_raw[i], mb[i]) for i in range(len(ts_raw)) if ts_raw[i] is not None
        )
        ts = [p[0] for p in pairs]
        mb = [p[1] for p in pairs]
        mode = "time"
    else:
        # Non-epoch time (like sample counter or string time)
        ts = list(range(len(mb)))
        mode = "index"

    # Write output
    out_dir = os.path.dirname(args.out) or "."
    os.makedirs(out_dir, exist_ok=True)
    with open(args.out, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["event_ts", "io_avg_MBps_pmW", "mode"])
        if mode == "time":
            for ev in det_ts:
                avg = moving_avg_by_time(ts, mb, ev, args.window)
                w.writerow([ev, f"{avg:.3f}" if not math.isnan(avg) else "nan", mode])
        else:
            # index mode: center around the middle index
            center = len(mb) // 2
            half = int(args.window)
            for ev in det_ts:
                avg = moving_avg_by_index(mb, center, half)
                w.writerow([ev, f"{avg:.3f}" if not math.isnan(avg) else "nan", mode])

    print(f"wrote: {args.out} (mode={mode})")

if __name__ == "__main__":
    main()
