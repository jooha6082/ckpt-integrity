#!/usr/bin/env python3
"""
Add a 'ts' (epoch seconds) column to trace/iostat.csv using a known start time
and sampling interval. This enables time-mode correlation.

Example:
  python tools/add_ts_to_iostat.py \
    --in trace/iostat.csv --out trace/iostat_with_ts.csv \
    --start-epoch 1696750000.0 --interval 1.0 --sample-col sample
"""
import argparse, csv

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="inp", default="trace/iostat.csv")
    ap.add_argument("--out", default="trace/iostat_with_ts.csv")
    ap.add_argument("--start-epoch", type=float, required=True, help="epoch seconds at sample=0")
    ap.add_argument("--interval", type=float, default=1.0, help="sampling period in seconds")
    ap.add_argument("--sample-col", default="sample", help="counter column name")
    args = ap.parse_args()

    rows = list(csv.DictReader(open(args.inp, newline="")))
    if not rows: raise SystemExit("iostat empty")

    hdr = ["ts"] + list(rows[0].keys())
    with open(args.out, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=hdr)
        w.writeheader()
        for r in rows:
            s = float(r[args.sample_col])
            row = {"ts": f"{args.start_epoch + s*args.interval:.6f}"}
            row.update(r)
            w.writerow(row)
    print("wrote:", args.out)

if __name__ == "__main__":
    main()
