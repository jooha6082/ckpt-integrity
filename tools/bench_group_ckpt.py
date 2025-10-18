#!/usr/bin/env python3
# Benchmark group checkpoint write latency across modes.
# - Accepts --seeds like "0-9", "0,1,5-7" or a single integer "10" (meaning 0..9)
# - Outputs:
#     figures/bench_group.csv
#     figures/bench_group_cdf.png
from __future__ import annotations
import argparse, sys, subprocess, time, re
from pathlib import Path
from typing import List, Tuple
import pandas as pd
import matplotlib.pyplot as plt

def parse_seeds(s: str) -> List[int]:
    """Parse seeds spec: '0-9', '0,2,5-7', or '10' (interpreted as 0..9)."""
    s = s.strip()
    if re.fullmatch(r"\d+", s):
        n = int(s)
        return list(range(n))
    out = set()
    for tok in s.split(","):
        tok = tok.strip()
        if not tok:
            continue
        if "-" in tok:
            a, b = tok.split("-", 1)
            a, b = int(a), int(b)
            lo, hi = (a, b) if a <= b else (b, a)
            out.update(range(lo, hi + 1))
        else:
            out.add(int(tok))
    return sorted(out)

def run(cmd: list[str]) -> float:
    print("[run]", " ".join(cmd))
    t0 = time.perf_counter()
    subprocess.run(cmd, check=True)
    return time.perf_counter() - t0

def one_case(py: str, out: str, epochs: int, every: int, seed: int,
             write_mode: str, dir_fsync: bool, kbmodel: int, kboptim: int, pause_ms: int
             ) -> Tuple[float, int, float]:
    args = [py, "-m", "src.aiwork.group_ckpt",
            "--out", out,
            "--epochs", str(epochs), "--every", str(every),
            "--seed", str(seed), "--write-mode", write_mode, "--fault", "none",
            "--kb-model", str(kbmodel), "--kb-optim", str(kboptim)]
    if pause_ms > 0:
        args += ["--pause-ms", str(pause_ms)]
    if write_mode == "atomic" and not dir_fsync:
        args.append("--no-dir-fsync")
    dt = run(args)
    n_ckpt = epochs // every
    per = dt / max(1, n_ckpt)
    return dt, n_ckpt, per

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--epochs", type=int, default=120)
    ap.add_argument("--every", type=int, default=3)
    ap.add_argument("--seeds", type=str, default="0-9")     # ← accepts "0-9"
    ap.add_argument("--kb-model", type=int, default=128)
    ap.add_argument("--kb-optim", type=int, default=64)
    ap.add_argument("--pause-ms", type=int, default=0)
    ap.add_argument("--out-csv", default="figures/bench_group.csv")
    ap.add_argument("--out-png", default="figures/bench_group_cdf.png")
    args = ap.parse_args()

    seeds = parse_seeds(args.seeds)
    py = sys.executable

    # (mode tag, write_mode, dir_fsync)
    modes = [
        ("unsafe", "unsafe", False),
        ("atomic_nodirsync", "atomic", False),
        ("atomic_dirsync", "atomic", True),
    ]

    rows = []
    for tag, wm, dirsync in modes:
        for s in seeds:
            out = f"trace/groups/bench_{tag}_s{s}"
            dt, n_ckpt, per = one_case(py, out, args.epochs, args.every, s, wm,
                                       dirsync, args.kb_model, args.kb_optim, args.pause_ms)
            rows.append({"mode": tag, "seed": s, "total_s": dt,
                         "per_ckpt_s": per, "n_ckpt": n_ckpt})

    df = pd.DataFrame(rows)
    Path(args.out_csv).parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(args.out_csv, index=False)
    print(f"[bench] wrote {args.out_csv}")

    # CDF of per-ckpt latency
    plt.figure(figsize=(7, 4))
    for tag, g in df.groupby("mode"):
        vals = sorted(g["per_ckpt_s"].tolist())
        y = [i/len(vals) for i in range(1, len(vals)+1)]
        plt.plot(vals, y, label=tag)
        # print quick quantiles for convenience
        q50 = g["per_ckpt_s"].quantile(0.50)
        q90 = g["per_ckpt_s"].quantile(0.90)
        q99 = g["per_ckpt_s"].quantile(0.99)
        print(f"[bench] {tag}: p50={q50:.4f}s p90={q90:.4f}s p99={q99:.4f}s (n={len(g)})")
    plt.xlabel("Per-checkpoint latency (s)")
    plt.ylabel("CDF")
    plt.title("Group checkpoint latency CDF")
    plt.legend()
    plt.tight_layout()
    plt.savefig(args.out_png, dpi=160)
    print(f"[bench] wrote {args.out_png}")

if __name__ == "__main__":
    main()
