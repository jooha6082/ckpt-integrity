#!/usr/bin/env python3
"""
Tiny FTL event simulator (stub) to unblock the pipeline.

Reads the cross-layer timeline, picks epoch timestamps, and emits a few
FTL/firmware events (ecc_bypass, ftl_map_corrupt, gc_delay) at fixed offsets
AFTER the chosen anchor epoch so the overlay shows non-zero markers.

Output CSV schema (same as parser expects):
  event_ts,event,delay_ms,retries
"""

import argparse, csv
from pathlib import Path
from typing import Dict, List, Optional, Tuple

# ---------- small I/O helpers ----------

def read_rows(path: Path) -> List[Dict[str, str]]:
    rows: List[Dict[str, str]] = []
    with path.open("r", newline="", errors="ignore") as f:
        r = csv.DictReader(f)
        for row in r:
            rows.append(row)
    return rows

def write_rows(path: Path, rows: List[Dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["event_ts","event","delay_ms","retries"])
        w.writeheader()
        for r in rows:
            w.writerow(r)

def f64(x: str) -> Optional[float]:
    try:
        return float(x)
    except Exception:
        return None

# ---------- timeline parsing ----------

def epoch_ts_map(timeline_csv: Path) -> Dict[int, float]:
    """Return {epoch_number: abs_ts} from src='app'/name='ckpt_write_mb' rows."""
    out: Dict[int, float] = {}
    for r in read_rows(timeline_csv):
        if r.get("src") != "app":
            continue
        if r.get("name") != "ckpt_write_mb":
            continue
        extra = (r.get("extra") or "")
        if not extra.startswith("epoch="):
            continue
        t = f64(r.get("ts_s",""))
        if t is None:
            continue
        try:
            n = int(extra.split("=",1)[1])
        except Exception:
            continue
        out[n] = t
    return out

# ---------- event synth ----------

def synth_events(ep: Dict[int,float], anchor_epoch: int) -> List[Dict[str,str]]:
    """
    Create a few deterministic events *after* anchor epoch so overlay is visible.
    Offsets are seconds from each epoch marker.
    """
    rows: List[Dict[str,str]] = []
    t4 = ep.get(anchor_epoch)
    if t4 is None:
        return rows

    # After epoch=4
    rows.append({"event_ts": f"{t4 + 120:.6f}", "event": "gc_delay",        "delay_ms": "40", "retries": "0"})
    rows.append({"event_ts": f"{t4 + 180:.6f}", "event": "ecc_bypass",      "delay_ms": "0",  "retries": "0"})

    # After epoch=5 (if exists)
    t5 = ep.get(5)
    if t5 is not None:
        rows.append({"event_ts": f"{t5 + 200:.6f}", "event": "ftl_map_corrupt","delay_ms": "0",  "retries": "2"})

    # After epoch=1 (if exists)
    t1 = ep.get(1)
    if t1 is not None:
        rows.append({"event_ts": f"{t1 + 60:.6f}", "event": "gc_delay",       "delay_ms": "25", "retries": "0"})

    # After epoch=2 (if exists)
    t2 = ep.get(2)
    if t2 is not None:
        rows.append({"event_ts": f"{t2 + 40:.6f}", "event": "ecc_bypass",     "delay_ms": "0",  "retries": "0"})

    # Sort by time just in case
    rows.sort(key=lambda r: float(r["event_ts"]))
    return rows

# ---------- CLI ----------

def main() -> int:
    ap = argparse.ArgumentParser(description="Emit a small FTL event CSV from timeline epochs (stub simulator).")
    ap.add_argument("--timeline", type=Path, default=Path("trace/xlayer_timeline.csv"))
    ap.add_argument("--anchor-epoch", type=int, default=4)
    ap.add_argument("--out", type=Path, default=Path("trace/ftl_events.csv"))
    args = ap.parse_args()

    epmap = epoch_ts_map(args.timeline)
    if not epmap:
        print(f"[ERR] no epoch markers found in {args.timeline}")
        return 2
    rows = synth_events(epmap, args.anchor_epoch)
    if not rows:
        print(f"[ERR] anchor epoch={args.anchor_epoch} not found in {args.timeline}")
        return 2

    write_rows(args.out, rows)
    print(f"[OK] wrote {len(rows)} events -> {args.out}")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
