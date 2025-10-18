#!/usr/bin/env python3
# Summarize bench_group.csv into quantiles and overhead vs unsafe.
from __future__ import annotations
import argparse
from pathlib import Path
import pandas as pd
import matplotlib.pyplot as plt

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--in-csv", default="figures/bench_group.csv")
    ap.add_argument("--out-summary", default="figures/bench_summary.csv")
    ap.add_argument("--out-overhead", default="figures/bench_overhead.csv")
    ap.add_argument("--out-md", default="figures/bench_summary.md")
    ap.add_argument("--out-png", default="figures/bench_bars.png")
    args = ap.parse_args()

    df = pd.read_csv(args.in_csv)
    if "mode" not in df.columns or "per_ckpt_s" not in df.columns:
        raise SystemExit("bench_group.csv missing required columns")

    # Quantiles per mode
    qs = (0.50, 0.90, 0.99)
    rows = []
    for mode, g in df.groupby("mode"):
        r = {"mode": mode, "n": len(g)}
        for q in qs:
            r[f"p{int(q*100)}"] = g["per_ckpt_s"].quantile(q)
        rows.append(r)
    summ = pd.DataFrame(rows).sort_values("mode")
    Path(args.out_summary).parent.mkdir(parents=True, exist_ok=True)
    summ.to_csv(args.out_summary, index=False)

    # Overhead % vs unsafe (for modes other than unsafe)
    if "unsafe" not in set(summ["mode"]):
        raise SystemExit("No 'unsafe' mode in bench; cannot compute overhead.")
    base = summ.set_index("mode").loc["unsafe"]
    ov_rows = []
    for _, r in summ.iterrows():
        if r["mode"] == "unsafe":
            continue
        ov_rows.append({
            "mode": r["mode"],
            "p50_overhead_pct": (r["p50"] - base["p50"]) / base["p50"] * 100.0,
            "p90_overhead_pct": (r["p90"] - base["p90"]) / base["p90"] * 100.0,
            "p99_overhead_pct": (r["p99"] - base["p99"]) / base["p99"] * 100.0,
        })
    over = pd.DataFrame(ov_rows).sort_values("mode")
    over.to_csv(args.out_overhead, index=False)

    # Markdown table (for report)
    lines = [
        "| mode | n | p50(s) | p90(s) | p99(s) |",
        "|:-----|--:|------:|------:|------:|",
    ]
    for _, r in summ.iterrows():
        lines.append(f"| {r['mode']} | {int(r['n'])} | {r['p50']:.6f} | {r['p90']:.6f} | {r['p99']:.6f} |")
    # Overhead section
    lines.append("\n**Overhead vs unsafe (%)**")
    lines.append("| mode | p50(%) | p90(%) | p99(%) |")
    lines.append("|:-----|------:|------:|------:|")
    for _, r in over.iterrows():
        lines.append(f"| {r['mode']} | {r['p50_overhead_pct']:.1f} | {r['p90_overhead_pct']:.1f} | {r['p99_overhead_pct']:.1f} |")
    Path(args.out_md).write_text("\n".join(lines), encoding="utf-8")

    # Bar chart: p50/p90/p99 grouped by mode
    modes = list(summ["mode"])
    width = 0.25
    import numpy as np
    x = np.arange(len(modes))
    plt.figure(figsize=(8,4))
    plt.bar(x - width, summ["p50"], width, label="p50")
    plt.bar(x,          summ["p90"], width, label="p90")
    plt.bar(x + width,  summ["p99"], width, label="p99")
    plt.xticks(x, modes, rotation=10)
    plt.ylabel("Per-ckpt latency (s)")
    plt.title("Group checkpoint latency (p50/p90/p99)")
    plt.legend()
    plt.tight_layout()
    plt.savefig(args.out_png, dpi=160)
    print(f"[bench] wrote {args.out_summary}, {args.out_overhead}, {args.out_md}, {args.out_png}")

if __name__ == "__main__":
    main()
