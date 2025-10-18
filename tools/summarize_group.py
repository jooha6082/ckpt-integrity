#!/usr/bin/env python3
# Summarize group-atomicity scans into a table and figures.
# Input : trace/guard/group_scan_all.csv  (from tools.run_group_fuzz)
# Output: figures/group_summary.csv, figures/group_summary.md,
#         figures/group_bars.png, figures/group_reasons.png
from __future__ import annotations
import argparse, math, re
from pathlib import Path
from typing import Tuple
import pandas as pd
import matplotlib.pyplot as plt

def wilson_ci(k:int, n:int, z:float=1.959963984540054) -> Tuple[float,float]:
    if n==0: return (0.0,0.0)
    p=k/n; denom=1+(z*z)/n
    center=(p+(z*z)/(2*n))/denom
    radius=(z*math.sqrt((p*(1-p)/n)+(z*z)/(4*n*n)))/denom
    lo=max(0.0, center-radius); hi=min(1.0, center+radius)
    return (lo,hi)

def parse_args():
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="inp", default="trace/guard/group_scan_all.csv")
    ap.add_argument("--out-csv", default="figures/group_summary.csv")
    ap.add_argument("--out-md", default="figures/group_summary.md")
    ap.add_argument("--out-png", default="figures/group_bars.png")
    ap.add_argument("--out-reasons", default="figures/group_reasons.png")
    return ap.parse_args()

def main():
    args=parse_args()
    df = pd.read_csv(args.inp)

    # Normalize columns
    for col in ["write_mode","crash_at","note"]:
        if col not in df.columns: df[col]="unknown"
    df["group_ok"] = df["group_ok"].astype(int)

    # --- Table aggregation ---
    g = df.groupby(["write_mode","crash_at"], as_index=False).agg(
        total=("group_ok","size"),
        ok=("group_ok","sum")
    )
    g["rate"]=g["ok"]/g["total"]
    cis = g.apply(lambda r: wilson_ci(int(r["ok"]), int(r["total"])), axis=1)
    g["ci_low"]=[c[0] for c in cis]; g["ci_high"]=[c[1] for c in cis]
    g["key"]=g["write_mode"]+"|"+g["crash_at"]

    # order for nicer plotting
    order={"atomic|none":10,
           "unsafe|after_model":20,"unsafe|before_manifest":30,
           "unsafe|manifest_partial":40,"unsafe|before_commit":50}
    g["order"]=g["key"].map(lambda k: order.get(k,999))
    g=g.sort_values(["order","write_mode","crash_at"]).reset_index(drop=True)

    Path(args.out_csv).parent.mkdir(parents=True, exist_ok=True)
    g.to_csv(args.out_csv, index=False)

    # Markdown table for report
    md=["| write | crash | total | group_ok | rate | 95% CI |",
        "|:------|:------|-----:|--------:|-----:|:-----:|"]
    for _,r in g.iterrows():
        md.append(f"| {r['write_mode']} | {r['crash_at']} | {int(r['total'])} | {int(r['ok'])} | {r['rate']:.3f} | [{r['ci_low']:.3f}, {r['ci_high']:.3f}] |")
    Path(args.out_md).write_text("\n".join(md), encoding="utf-8")
    print(f"[table] wrote {args.out_csv} and {args.out_md}")

    # --- Bar chart with 95% CI ---
    x=list(g["key"]); y=list(g["rate"])
    yerr_low=[max(0.0,m-lo) for m,lo in zip(y,g["ci_low"])]
    yerr_high=[max(0.0,hi-m) for m,hi in zip(y,g["ci_high"])]
    yerr=[yerr_low,yerr_high]

    plt.figure(figsize=(10,4))
    plt.bar(range(len(x)), y, yerr=yerr, capsize=3)
    plt.xticks(range(len(x)), x, rotation=25, ha="right")
    plt.ylabel("Group OK rate")
    plt.title("Group atomicity by write|crash (95% CI)")
    plt.tight_layout()
    plt.savefig(args.out_png, dpi=160)
    print(f"[figure] wrote {args.out_png}")

    # --- Reason breakdown (stacked bars) ---
    # Parse note field for primary reasons
    def has(pat: str) -> pd.Series:
        return df["note"].fillna("").str.contains(pat, regex=True)
    df["no_commit"] = has(r"\bno_commit\b").astype(int)
    df["commit_manifest_mismatch"] = has(r"\bcommit_manifest_mismatch\b").astype(int)
    df["manifest_error"] = has(r"\bmanifest_error").astype(int)
    df["missing_part"] = has(r"\bmissing:").astype(int)
    df["size_mismatch"] = has(r"\bsize_mismatch:").astype(int)
    df["sha_mismatch"] = has(r"\bsha_mismatch:").astype(int)

    g2 = df.groupby(["write_mode","crash_at"], as_index=False).agg(
        total=("group_ok","size"),
        no_commit=("no_commit","sum"),
        commit_manifest_mismatch=("commit_manifest_mismatch","sum"),
        manifest_error=("manifest_error","sum"),
        missing_part=("missing_part","sum"),
        size_mismatch=("size_mismatch","sum"),
        sha_mismatch=("sha_mismatch","sum"),
    )
    g2["key"]=g2["write_mode"]+"|"+g2["crash_at"]
    g2=g2.sort_values("key")

    # stacked bars
    reasons=["no_commit","commit_manifest_mismatch","manifest_error","missing_part","size_mismatch","sha_mismatch"]
    plt.figure(figsize=(10,4))
    bottom=None
    for r in reasons:
        vals=list(g2[r])
        if bottom is None:
            plt.bar(g2["key"], vals, label=r)
            bottom=vals
        else:
            plt.bar(g2["key"], vals, bottom=bottom, label=r)
            bottom=[a+b for a,b in zip(bottom, vals)]
    plt.xticks(rotation=25, ha="right")
    plt.ylabel("Count")
    plt.title("Failure reason breakdown (by write|crash)")
    plt.legend(fontsize=8, ncol=3)
    plt.tight_layout()
    plt.savefig(args.out_reasons, dpi=160)
    print(f"[figure] wrote {args.out_reasons}")

if __name__ == "__main__":
    main()
