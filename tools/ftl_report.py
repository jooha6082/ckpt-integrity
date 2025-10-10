#!/usr/bin/env python3
"""
Render a compact human-readable report for FTL/firmware events.

Inputs
  --overall  CSV from ftl_event_parser.py (xlayer_event_summary.csv)
             columns: event,count,sum_delay_ms,sum_retries
  --epoch    CSV from ftl_epoch_buckets.py (ftl_epoch_summary.csv)
             one row per epoch window with per-event stats

Output
  --out      Plain-text report summarizing totals and per-epoch windows.

"""

from __future__ import annotations
import argparse, csv
from pathlib import Path
from typing import Dict, List

# ---------------- I/O helpers ----------------

def read_csv_rows(path: Path) -> List[Dict[str, str]]:
    if not path.exists():
        return []
    rows: List[Dict[str, str]] = []
    with path.open("r", newline="", errors="ignore") as f:
        r = csv.DictReader(f)
        for row in r:
            rows.append(row)
    return rows

def wlines(path: Path, lines: List[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n")

def i64(s: str) -> int:
    try:
        return int(float(s))
    except Exception:
        return 0

# ---------------- render ----------------

EVENTS = ("ecc_bypass", "ftl_map_corrupt", "gc_delay")

def render_report(overall_csv: Path, epoch_csv: Path) -> List[str]:
    lines: List[str] = []
    overall = read_csv_rows(overall_csv)
    epoch = read_csv_rows(epoch_csv)

    lines.append("=== FTL/Firmware Event Summary ===")

    # ---- totals ----
    if overall:
        lines.append("")
        lines.append("[Totals]")
        hdr = f"{'event':<16} {'count':>7} {'sum_delay_ms':>14} {'sum_retries':>12}"
        lines.append(hdr)
        lines.append("-" * len(hdr))
        total_count = total_delay = total_retries = 0
        for r in overall:
            ev = (r.get("event") or "")
            cnt = i64(r.get("count","0"))
            dms = i64(r.get("sum_delay_ms","0"))
            rty = i64(r.get("sum_retries","0"))
            lines.append(f"{ev:<16} {cnt:>7} {dms:>14} {rty:>12}")
            total_count += cnt; total_delay += dms; total_retries += rty
        lines.append("-" * len(hdr))
        lines.append(f"{'TOTAL':<16} {total_count:>7} {total_delay:>14} {total_retries:>12}")
    else:
        lines.append("")
        lines.append("[Totals]")
        lines.append("No overall CSV found or it is empty.")

    # ---- per-epoch windows ----
    lines.append("")
    lines.append("[Per-epoch windows]")
    if not epoch:
        lines.append("No epoch summary CSV found or it is empty.")
        return lines

    # Only show non-zero rows first; if all zero, say so explicitly.
    nonzero_rows: List[Dict[str,str]] = []
    for r in epoch:
        if i64(r.get("total_count","0")) > 0:
            nonzero_rows.append(r)

    if not nonzero_rows:
        # Still print the window layout so readers know the boundaries.
        lines.append("All windows: 0 events.")
        lines.append("")
        lines.append(f"{'win':>3}  {'range(s)':<25}  {'start->end':<18}  {'dur(s)':>8}")
        lines.append("-"*64)
        for r in epoch:
            idx = r.get("win_idx","")
            st = r.get("start_rel_s","")
            en = r.get("end_rel_s","")
            dur = r.get("duration_s","")
            se = f"{r.get('start_epoch','')} -> {r.get('end_epoch','') or '(end)'}"
            rng = f"[{st}, {en or '...'} )"
            lines.append(f"{idx:>3}  {rng:<25}  {se:<18}  {dur:>8}")
        return lines

    # Print only the windows that have activity, with per-event stats.
    for r in nonzero_rows:
        idx = r.get("win_idx","")
        se = f"{r.get('start_epoch','')} -> {r.get('end_epoch','') or '(end)'}"
        st = r.get("start_rel_s","")
        en = r.get("end_rel_s","")
        dur = r.get("duration_s","")
        lines.append("")
        lines.append(f"- Window #{idx}  {se}  range=[{st}, {en or '...'}), dur={dur or '...'} s")
        lines.append(f"  {'event':<16} {'count':>7} {'sum_delay_ms':>14} {'sum_retries':>12}")
        for ev in EVENTS:
            cnt = r.get(f"{ev}_count","0")
            dms = r.get(f"{ev}_sum_delay_ms","0")
            rty = r.get(f"{ev}_sum_retries","0")
            lines.append(f"  {ev:<16} {cnt:>7} {dms:>14} {rty:>12}")
        lines.append(f"  {'TOTAL':<16} {r.get('total_count','0'):>7} {r.get('total_sum_delay_ms','0'):>14} {r.get('total_sum_retries','0'):>12}")

    return lines

# ---------------- CLI ----------------

def main() -> int:
    ap = argparse.ArgumentParser(description="Render a text report for FTL events (totals + per-epoch windows).")
    ap.add_argument("--overall", type=Path, default=Path("trace/xlayer_event_summary.csv"))
    ap.add_argument("--epoch", type=Path, default=Path("trace/ftl_epoch_summary.csv"))
    ap.add_argument("--out", type=Path, default=Path("trace/events_summary.txt"))
    args = ap.parse_args()

    lines = render_report(args.overall, args.epoch)
    wlines(args.out, lines)
    print(f"[OK] wrote report -> {args.out}")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
