#!/usr/bin/env python3
"""
Plot cross-layer timeline:
- iostat tps as a line over relative time (seconds since first event)
- app checkpoint_saved events as vertical markers

No seaborn; plain matplotlib only.
"""
from __future__ import annotations
import argparse, os
from pathlib import Path
import matplotlib.pyplot as plt
import pandas as pd

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--timeline", default="trace/timeline/timeline.csv")
    ap.add_argument("--out", default="figures/timeline.png")
    args = ap.parse_args()

    df = pd.read_csv(args.timeline)
    if df.empty:
        raise SystemExit("Empty timeline CSV")

    t0 = df["ts_s"].min()
    df["t_rel"] = df["ts_s"] - t0

    io = df[df["src"]=="iostat"].copy()
    io["tps"] = pd.to_numeric(io["value"], errors="coerce")
    io = io.dropna(subset=["tps"])

    app = df[(df["src"]=="app") & (df["name"]=="checkpoint_saved")].copy()

    Path(args.out).parent.mkdir(parents=True, exist_ok=True)

    # Plot
    plt.figure(figsize=(10,4))
    if not io.empty:
        plt.plot(io["t_rel"], io["tps"], linewidth=1.5, label="iostat tps")
    # vertical markers for checkpoints
    for i, r in app.iterrows():
        plt.axvline(r["t_rel"], linewidth=0.6, linestyle="--")

    plt.xlabel("Time since start (s)")
    plt.ylabel("iostat tps")
    plt.title("Cross-layer Timeline: iostat vs. checkpoint events")
    plt.tight_layout()
    plt.savefig(args.out, dpi=160)
    print(f"[plot] wrote {args.out}")

if __name__ == "__main__":
    main()
