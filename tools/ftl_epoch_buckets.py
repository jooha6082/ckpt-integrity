#!/usr/bin/env python3
"""
Bucket FTL/firmware events by epoch windows on the cross-layer timeline.

Inputs
------
- timeline CSV (per-event rows):
    ts_s,src,name,value,device,extra
  We only use src='app' rows with extra like 'epoch=N'. Others are ignored.

- events CSV (standardized FTL events, from ftl_event_parser.py):
    event_ts,event,delay_ms,retries

Behavior
--------
- Anchor time at epoch={anchor_epoch} -> t0.
- Trim-left: only consider t >= 0.
- Build epoch windows between consecutive app epoch markers, e.g.:
    [epoch=4, epoch=5), [epoch=5, epoch=1), [epoch=1, epoch=2), ...
  The last window is open-ended: [last, +inf).

- For each window, aggregate per-event-type stats:
    count, sum_delay_ms, sum_retries
  for {ecc_bypass, ftl_map_corrupt, gc_delay}, plus totals.

Output
------
A CSV with one row per window:
  win_idx,start_epoch,end_epoch,start_rel_s,end_rel_s,duration_s,
  ecc_bypass_count,ecc_bypass_sum_delay_ms,ecc_bypass_sum_retries,
  ftl_map_corrupt_count,ftl_map_corrupt_sum_delay_ms,ftl_map_corrupt_sum_retries,
  gc_delay_count,gc_delay_sum_delay_ms,gc_delay_sum_retries,
  total_count,total_sum_delay_ms,total_sum_retries
"""

from __future__ import annotations
import argparse
import csv
from pathlib import Path
from typing import Dict, List, Optional, Tuple

# ------------- tiny utils -------------

def f64(x: str) -> Optional[float]:
    try:
        return float(x)
    except Exception:
        return None

def i64(x: str) -> int:
    try:
        return int(float(x))
    except Exception:
        return 0

def read_rows(path: Path) -> List[Dict[str, str]]:
    rows: List[Dict[str, str]] = []
    with path.open("r", newline="", errors="ignore") as f:
        r = csv.DictReader(f)
        for row in r:
            rows.append(row)
    return rows

def write_rows(path: Path, rows: List[Dict[str, str]], fieldnames: List[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            w.writerow(r)


# ------------- timeline helpers -------------

def pick_anchor_t0(timeline: List[Dict[str, str]], anchor_epoch: int) -> Optional[float]:
    key = f"epoch={anchor_epoch}"
    for r in timeline:
        if r.get("src") == "app" and r.get("name") == "ckpt_write_mb":
            if key in (r.get("extra") or ""):
                t = f64(r.get("ts_s", ""))
                if t is not None:
                    return t
    return None

def get_epoch_markers(timeline: List[Dict[str, str]]) -> List[Tuple[float, str]]:
    """
    Return sorted [(abs_ts, 'epoch=4'), ...] for src='app' where extra startswith 'epoch='.
    """
    out: List[Tuple[float, str]] = []
    for r in timeline:
        if r.get("src") != "app":
            continue
        lab = (r.get("extra") or "")
        if not lab.startswith("epoch="):
            continue
        t = f64(r.get("ts_s", ""))
        if t is None:
            continue
        out.append((t, lab))
    out.sort(key=lambda x: x[0])
    return out

def rel_trim_epoch_markers(markers: List[Tuple[float,str]], t0: Optional[float]) -> List[Tuple[float,str]]:
    """
    Convert to relative seconds and drop t<0.
    """
    out: List[Tuple[float,str]] = []
    for t, lab in markers:
        rel = t - t0 if t0 is not None else t
        if rel >= 0.0:
            out.append((rel, lab))
    return out

def build_windows(eps_rel: List[Tuple[float,str]]) -> List[Tuple[float, Optional[float], str, Optional[str]]]:
    """
    From sorted relative epoch markers -> windows:
      [(start_s, end_s_or_None, start_label, end_label_or_None), ...]
    """
    wins: List[Tuple[float, Optional[float], str, Optional[str]]] = []
    if not eps_rel:
        return wins
    for i in range(len(eps_rel)):
        t0, lab0 = eps_rel[i]
        if i + 1 < len(eps_rel):
            t1, lab1 = eps_rel[i + 1]
            wins.append((t0, t1, lab0, lab1))
        else:
            wins.append((t0, None, lab0, None))
    return wins


# ------------- bucketing -------------

EVS = ("ecc_bypass", "ftl_map_corrupt", "gc_delay")

def init_bucket_row(idx: int, start: float, end: Optional[float],
                    lab0: str, lab1: Optional[str]) -> Dict[str, str]:
    dur = (end - start) if end is not None else None
    row: Dict[str, str] = {
        "win_idx": str(idx),
        "start_epoch": lab0,
        "end_epoch": lab1 or "",
        "start_rel_s": f"{start:.6f}",
        "end_rel_s": (f"{end:.6f}" if end is not None else ""),
        "duration_s": (f"{dur:.6f}" if dur is not None else ""),
    }
    for ev in EVS:
        row[f"{ev}_count"] = "0"
        row[f"{ev}_sum_delay_ms"] = "0"
        row[f"{ev}_sum_retries"] = "0"
    row["total_count"] = "0"
    row["total_sum_delay_ms"] = "0"
    row["total_sum_retries"] = "0"
    return row

def add_event(row: Dict[str, str], ev: str, delay_ms: int, retries: int) -> None:
    # per-event-type
    row[f"{ev}_count"] = str(int(row[f"{ev}_count"]) + 1)
    row[f"{ev}_sum_delay_ms"] = str(int(row[f"{ev}_sum_delay_ms"]) + delay_ms)
    row[f"{ev}_sum_retries"] = str(int(row[f"{ev}_sum_retries"]) + retries)
    # totals
    row["total_count"] = str(int(row["total_count"]) + 1)
    row["total_sum_delay_ms"] = str(int(row["total_sum_delay_ms"]) + delay_ms)
    row["total_sum_retries"] = str(int(row["total_sum_retries"]) + retries)

def bucket_events(
    events: List[Dict[str, str]],
    t0: float,
    windows: List[Tuple[float, Optional[float], str, Optional[str]]],
) -> List[Dict[str, str]]:
    """
    Assign each FTL event (t_rel >= 0) to exactly one window [start, end).
    The last window is open-ended.
    """
    # Prepare rows
    rows: List[Dict[str, str]] = [
        init_bucket_row(i, start, end, lab0, lab1)
        for i, (start, end, lab0, lab1) in enumerate(windows)
    ]
    if not rows:
        return rows

    # Process events
    for r in events:
        t_abs = f64(r.get("event_ts", ""))
        ev = (r.get("event") or "").lower()
        if t_abs is None or ev not in EVS:
            continue
        t_rel = t_abs - t0
        if t_rel < 0.0:
            continue  # trim-left

        delay_ms = i64(r.get("delay_ms", "0"))
        retries = i64(r.get("retries", "0"))

        # find window
        idx = None
        for i, (start, end, _lab0, _lab1) in enumerate(windows):
            if end is None:
                if t_rel >= start:
                    idx = i
                    break
            else:
                if start <= t_rel < end:
                    idx = i
                    break
        if idx is None:
            # Should not happen, but guard anyway: put into the last window
            idx = len(rows) - 1

        add_event(rows[idx], ev, delay_ms, retries)

    return rows


# ------------- CLI -------------

def main() -> int:
    ap = argparse.ArgumentParser(description="Bucket FTL events by epoch windows.")
    ap.add_argument("--timeline", type=Path, default=Path("trace/xlayer_timeline.csv"),
                    help="Per-event timeline CSV (ts_s,src,name,value,device,extra)")
    ap.add_argument("--events", type=Path, default=Path("trace/ftl_events.csv"),
                    help="FTL events CSV (event_ts,event,delay_ms,retries)")
    ap.add_argument("--anchor-epoch", type=int, default=4,
                    help="Anchor epoch for t=0 (default: 4)")
    ap.add_argument("--out", type=Path, default=Path("trace/ftl_epoch_summary.csv"))
    args = ap.parse_args()

    if not args.timeline.exists():
        print(f"[ERR] not found timeline: {args.timeline}")
        return 2

    tl = read_rows(args.timeline)
    if not tl or "ts_s" not in tl[0]:
        print(f"[ERR] timeline is not per-event format (needs ts_s).")
        return 2

    t0 = pick_anchor_t0(tl, args.anchor_epoch)
    if t0 is None:
        print(f"[ERR] cannot find anchor epoch={args.anchor_epoch} in timeline.")
        return 2

    markers_abs = get_epoch_markers(tl)
    markers_rel = rel_trim_epoch_markers(markers_abs, t0)
    if not markers_rel:
        print("[ERR] no epoch markers after trim-left.")
        return 2

    wins = build_windows(markers_rel)

    ev_rows = read_rows(args.events) if args.events.exists() else []
    out_rows = bucket_events(ev_rows, t0, wins)

    # write CSV
    fieldnames = [
        "win_idx","start_epoch","end_epoch","start_rel_s","end_rel_s","duration_s",
        "ecc_bypass_count","ecc_bypass_sum_delay_ms","ecc_bypass_sum_retries",
        "ftl_map_corrupt_count","ftl_map_corrupt_sum_delay_ms","ftl_map_corrupt_sum_retries",
        "gc_delay_count","gc_delay_sum_delay_ms","gc_delay_sum_retries",
        "total_count","total_sum_delay_ms","total_sum_retries",
    ]
    write_rows(args.out, out_rows, fieldnames)
    print(f"[OK] wrote bucket summary -> {args.out} (windows={len(out_rows)})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
