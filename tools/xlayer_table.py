
"""
Summarize a unified cross-layer timeline CSV with flexible group keys.

Input CSV (from tools/xlayer_timeline.py --out):
  ts_s,src,name,value,device,extra

Examples
  # default grouping (src,name,device)
  python tools/xlayer_table.py --timeline trace/xlayer_timeline_full.csv --out trace/xlayer_table.csv

  # group by just 'src' (quick check that fs/app/iostat arrived)
  python tools/xlayer_table.py --timeline trace/xlayer_timeline_full.csv --by src --out trace/xlayer_by_src.csv

  # split app rows by epoch extracted from 'extra'
  python tools/xlayer_table.py --timeline trace/xlayer_timeline_full.csv --by src,name,device,epoch --out trace/xlayer_table_epoch.csv

  # group by exact 'extra' string
  python tools/xlayer_table.py --timeline trace/xlayer_timeline_full.csv --by src,name,device,extra --out trace/xlayer_table_extra.csv
"""

import argparse
import csv
import re
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

EPOCH_RE = re.compile(r"(?:^|[;,\s])epoch=(\d+)(?:[;,\s]|$)")

# ----------------------------
# IO helpers
# ----------------------------

def read_rows(path: Path) -> List[Dict[str, str]]:
    """Load unified timeline rows from CSV."""
    rows: List[Dict[str, str]] = []
    with path.open("r", newline="") as f:
        r = csv.DictReader(f)
        for row in r:
            rows.append(row)
    return rows

def to_float(x: str) -> Optional[float]:
    """Parse float safely; return None on failure."""
    try:
        return float(x)
    except Exception:
        return None

# ----------------------------
# Grouping helpers
# ----------------------------

def extract_epoch(extra: str) -> str:
    """Return epoch number as string if found in 'extra'; else empty."""
    if not extra:
        return ""
    m = EPOCH_RE.search(extra)
    return m.group(1) if m else ""

def group_key(row: Dict[str, str], keys: Sequence[str]) -> Tuple[str, ...]:
    """Build a composite key from requested fields."""
    out: List[str] = []
    for k in keys:
        if k == "epoch":
            out.append(extract_epoch(row.get("extra", "")))
        else:
            out.append(row.get(k, "") or "")
    return tuple(out)

def summarize(rows: List[Dict[str, str]], keys: Sequence[str]) -> List[Dict[str, str]]:
    """Aggregate numeric 'value' by groups; emit count/min/max/mean."""
    from math import fsum
    buckets: Dict[Tuple[str, ...], List[float]] = {}
    for r in rows:
        v = to_float(r.get("value", ""))
        if v is None:
            continue
        buckets.setdefault(group_key(r, keys), []).append(v)

    out_rows: List[Dict[str, str]] = []
    for k, arr in sorted(buckets.items()):
        cnt = len(arr)
        vmin = min(arr)
        vmax = max(arr)
        mean = fsum(arr) / cnt if cnt else 0.0
        rec = {keys[i]: k[i] for i in range(len(keys))}
        rec.update({
            "count": str(cnt),
            "min": f"{vmin:.6f}",
            "max": f"{vmax:.6f}",
            "mean": f"{mean:.6f}",
        })
        out_rows.append(rec)
    return out_rows

def write_csv(path: Path, rows: Iterable[Dict[str, str]], keys: Sequence[str]) -> None:
    """Write table rows using keys + metrics as columns."""
    cols = list(keys) + ["count", "min", "max", "mean"]
    with path.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        for r in rows:
            w.writerow({c: r.get(c, "") for c in cols})

# ----------------------------
# CLI
# ----------------------------

def main() -> int:
    ap = argparse.ArgumentParser(description="Summarize a cross-layer timeline with flexible group keys.")
    ap.add_argument("--timeline", type=Path, required=True, help="input CSV path from xlayer_timeline.py --out")
    ap.add_argument("--by", type=str, default="src,name,device",
                    help="comma-separated keys among: src,name,device,extra,epoch (default: src,name,device)")
    ap.add_argument("--out", type=Path, help="optional output CSV path; if omitted, only preview")
    ap.add_argument("--limit", type=int, default=20, help="preview limit when --out is not set")
    args = ap.parse_args()

    keys = [s.strip() for s in args.by.split(",") if s.strip()]
    rows = read_rows(args.timeline)
    tbl = summarize(rows, keys)

    if args.out:
        args.out.parent.mkdir(parents=True, exist_ok=True)
        write_csv(args.out, tbl, keys)
        print(f"wrote: {args.out} ({len(tbl)} rows)")
    else:
        print(f"[preview] groups: {len(tbl)}  by={keys}")
        for r in tbl[: max(0, args.limit)]:
            print(r)

    return 0

if __name__ == "__main__":
    raise SystemExit(main())
