#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Batch runner to grow samples via multiple seeds/modes and crash/write-mode variants.

Examples:
  # Baseline only
  python tools/run_many.py --seeds 0-9 --epochs 60 --every 3 --modes none

  # Fault matrix (silent vs loud corruption)
  python tools/run_many.py --seeds 0-9 --epochs 60 --every 3 \
      --modes none,bitflip,truncate,zerorange

  # Crash consistency (unsafe+crash at early/mid/late)
  python tools/run_many.py --seeds 0-9 --epochs 60 --every 3 \
      --modes none --write-mode unsafe --crash early,mid,late
"""
from __future__ import annotations
import argparse, subprocess, sys
from pathlib import Path
import pandas as pd


def parse_range(s: str) -> list[int]:
    parts = []
    for tok in s.split(","):
        tok = tok.strip()
        if not tok:
            continue
        if "-" in tok:
            a, b = tok.split("-", 1)
            a, b = int(a), int(b)
            lo, hi = min(a, b), max(a, b)
            parts.extend(range(lo, hi + 1))
        else:
            parts.append(int(tok))
    return sorted(set(parts))


def ckpt_epochs(epochs: int, every: int) -> list[int]:
    return list(range(every, epochs + 1, every))


def choose_crash_epoch(epochs: int, every: int, when: str) -> int:
    cks = ckpt_epochs(epochs, every)
    if not cks: return -1
    if when == "early":  return cks[0]
    if when == "mid":    return cks[len(cks)//2]
    if when == "late":   return cks[-1]
    return -1


def run_ckpt_writer(py, out_dir: Path, epochs: int, every: int,
                    seed: int, mode: str, write_mode: str, crash_epoch: int) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    cmd = [
        py, "-m", "src.aiwork.ckpt_writer",
        "--epochs", str(epochs),
        "--checkpoint-every", str(every),
        "--out", str(out_dir),
        "--seed", str(seed),
        "--fault", mode,
        "--write-mode", write_mode,
    ]
    if crash_epoch > 0:
        cmd += ["--crash-epoch", str(crash_epoch)]
    print("[run] ", " ".join(map(str, cmd)))
    subprocess.run(cmd, check=False)  # allow crash (exit 1)


def run_guard(py, ckpt_dir: Path, out_csv: Path) -> None:
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    cmd = [py, "-m", "src.guard.integrity_guard",
           "--ckpt-dir", str(ckpt_dir), "--out", str(out_csv)]
    print("[scan]", " ".join(map(str, cmd)))
    subprocess.run(cmd, check=True)


def aggregate_csv(per_run_csvs: list[Path], out_all: Path) -> None:
    frames = [pd.read_csv(p) for p in per_run_csvs if p.exists()]
    if not frames:
        print("[agg] no inputs found")
        return
    out_all.parent.mkdir(parents=True, exist_ok=True)
    pd.concat(frames, ignore_index=True).to_csv(out_all, index=False)
    print(f"[agg] wrote {out_all}")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--seeds", default="0-9", help="e.g., 0-9 or 0,1,7,9-10")
    ap.add_argument("--epochs", type=int, default=60)
    ap.add_argument("--every", type=int, default=3)
    ap.add_argument("--modes", default="none", help="comma list: none,bitflip,truncate,zerorange")
    ap.add_argument("--write-mode", default="atomic", choices=["atomic", "unsafe"])
    ap.add_argument("--crash", default="none", help="comma list: none,early,mid,late")
    ap.add_argument("--root", default="trace/ckpts_runs")
    ap.add_argument("--scan-root", default="trace/guard/runs")
    ap.add_argument("--agg-out", default="trace/guard/ckpt_scan_all.csv")
    args = ap.parse_args()

    seeds = parse_range(args.seeds)
    modes = [m.strip() for m in args.modes.split(",") if m.strip()]
    crash_modes = [c.strip() for c in args.crash.split(",") if c.strip()]
    if not crash_modes:
        crash_modes = ["none"]

    py = sys.executable
    per_run_csvs: list[Path] = []

    for mode in modes:
        for crash in crash_modes:
            for seed in seeds:
                crash_epoch = choose_crash_epoch(args.epochs, args.every, crash) if crash != "none" else -1
                write_tag = getattr(args, "write_mode", "atomic")  # default fallback
                crash_tag = crash if crash else "nocrash"          # be robust if crash can be None
                run_dir = Path(args.root) / f"{mode}__{write_tag}__{crash_tag}" / f"seed_{seed}"
                run_ckpt_writer(py, run_dir, args.epochs, args.every,
                                seed, mode, args.write_mode, crash_epoch)
                scan_csv = Path(args.scan_root) / f"{mode}__{args.write_mode}__{crash}" / f"seed_{seed}.csv"
                run_guard(py, run_dir, scan_csv)
                per_run_csvs.append(scan_csv)

    aggregate_csv(per_run_csvs, Path(args.agg_out))


if __name__ == "__main__":
    main()
