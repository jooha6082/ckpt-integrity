#!/usr/bin/env python3
# Select latest good group checkpoint (group_ok=1) from a scan CSV
# and atomically update a symlink (out-link) to that epoch directory.
from __future__ import annotations
import argparse, csv, os, tempfile
from pathlib import Path

def pick_latest_ok(scan_csv: Path) -> Path | None:
    best_epoch = -1
    best_dir: Path | None = None
    with open(scan_csv, newline="") as f:
        r = csv.DictReader(f)
        for row in r:
            try:
                ok = int(row.get("group_ok", "0"))
                ep = int(row.get("epoch", "-1"))
                d  = row.get("dir", "")
            except Exception:
                continue
            if ok == 1 and ep > best_epoch:
                best_epoch = ep
                best_dir = Path(d)
    return best_dir

def atomic_update_symlink(link_path: Path, target: Path) -> None:
    link_path.parent.mkdir(parents=True, exist_ok=True)
    tmp = link_path.parent / (".tmp_link_" + next(tempfile._get_candidate_names()))
    # relative symlink for portability
    rel = os.path.relpath(target, start=link_path.parent)
    os.symlink(rel, tmp)
    try:
        os.replace(tmp, link_path)  # atomic on POSIX
    except Exception:
        try: link_path.unlink()
        except FileNotFoundError: pass
        os.replace(tmp, link_path)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--scan", required=True, help="CSV produced by group_guard")
    ap.add_argument("--out-link", required=True, help="symlink to update")
    args = ap.parse_args()

    scan_csv = Path(args.scan)
    if not scan_csv.exists():
        raise SystemExit(f"scan CSV not found: {scan_csv}")

    target = pick_latest_ok(scan_csv)
    if not target:
        raise SystemExit("no group_ok=1 rows found")
    atomic_update_symlink(Path(args.out_link), target)
    print(f"[rollback] linked {args.out_link} -> {target}")

if __name__ == "__main__":
    main()
