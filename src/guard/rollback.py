#!/usr/bin/env python3
"""
Pick the most recent non-corrupted checkpoint and create/update a symlink.

Rules:
- Choose the largest epoch with corrupted==0.
- Validate the file exists under --ckpt-root.
- Create/replace a symlink (--out-link) pointing to that file.

Usage:
  python -m src.guard.rollback --scan-csv trace/guard/ckpt_scan.csv \
      --ckpt-root trace/ckpts --out-link trace/ckpts/latest_ok.npz
"""
from __future__ import annotations
import argparse, csv, os
from pathlib import Path

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--scan-csv", required=True)
    ap.add_argument("--ckpt-root", required=True)
    ap.add_argument("--out-link", required=True)
    args = ap.parse_args()

    rows=[]
    with open(args.scan_csv, newline="") as f:
        r=csv.DictReader(f)
        for row in r:
            try:
                row["epoch"]=int(row.get("epoch","-1"))
                row["corrupted"]=int(row.get("corrupted","1"))
            except Exception:
                continue
            rows.append(row)

    ok = [r for r in rows if r["corrupted"]==0 and r["epoch"]>=0 and r.get("file")]
    if not ok:
        raise SystemExit("No non-corrupted checkpoints found")

    best = max(ok, key=lambda r: r["epoch"])
    ckpt_path = Path(args.ckpt_root) / best["file"]
    if not ckpt_path.exists():
        raise SystemExit(f"Checkpoint not found on disk: {ckpt_path}")

    out_link = Path(args.out_link)
    out_link.parent.mkdir(parents=True, exist_ok=True)
    try:
        if out_link.exists() or out_link.is_symlink():
            out_link.unlink()
    except FileNotFoundError:
        pass
    os.symlink(ckpt_path.resolve(), out_link)
    print(f"[rollback] linked {out_link} -> {ckpt_path}")

if __name__ == "__main__":
    main()
