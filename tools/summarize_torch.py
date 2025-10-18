#!/usr/bin/env python3
# Summarize Torch runs into a table (CSV/MD) and a bar chart with 95% CI.
# - Scans: trace/guard/runs_torch/*/*.csv
# - Outputs:
#     figures/torch_mode_summary.csv
#     figures/torch_mode_summary.md
#     figures/torch_mode_bars.png
from __future__ import annotations
import argparse, math, re
from pathlib import Path
from typing import Dict, List, Tuple
import pandas as pd
import matplotlib.pyplot as plt

def wilson_ci(k: int, n: int, z: float = 1.959963984540054) -> Tuple[float, float]:
    """Wilson score 95% CI for binomial proportion."""
    if n == 0:
        return (0.0, 0.0)
    p = k / n
    denom = 1.0 + (z*z)/n
    center = (p + (z*z)/(2*n)) / denom
    radius = (z * math.sqrt((p*(1-p)/n) + (z*z)/(4*n*n))) / denom
    lo, hi = max(0.0, center - radius), min(1.0, center + radius)
    return (lo, hi)

def scan_runs(root: str = "trace/guard/runs_torch") -> pd.DataFrame:
    rows: List[pd.DataFrame] = []
    for csv_path in sorted(Path(root).glob("*/*.csv")):
        # parse <mode>__<write>__<crash>/seed_X.csv
        parts = csv_path.as_posix().split("/")
        triplet = parts[-2]  # "<mode>__<write_mode>__<crash>"
        try:
            mode, write_mode, crash = triplet.split("__")
        except ValueError:
            # fallback: unknown naming
            mode, write_mode, crash = "unknown", "unknown", "unknown"
        seed = re.sub(r"^seed_", "", Path(parts[-1]).stem)

        df = pd.read_csv(csv_path)
        df["mode"] = mode
        df["write_mode"] = write_mode
        df["crash"] = crash
        df["seed"] = seed
        df["key"] = df["mode"] + "|" + df["write_mode"] + "|" + df["crash"]
        rows.append(df)
    if not rows:
        raise SystemExit(f"No CSV files under {root}")
    return pd.concat(rows, ignore_index=True)

def summarize(df: pd.DataFrame) -> pd.DataFrame:
    # Robust note parsing (may be empty)
    def has_note(s: pd.Series, pat: str) -> pd.Series:
        return s.fillna("").str.contains(pat, regex=True)

    df["is_corrupted"] = (df["corrupted"] > 0).astype(int)
    df["n_load_error"]      = has_note(df["note"], r"\bload_error\b").astype(int)
    df["n_digest_mismatch"] = has_note(df["note"], r"\bdigest_mismatch\b").astype(int)
    df["n_file_sha_mismatch"] = has_note(df["note"], r"\bfile_sha_mismatch\b").astype(int)

    grp = df.groupby(["mode", "write_mode", "crash"], as_index=False)
    # Basic counts
    agg = grp.agg(
        total=("is_corrupted", "size"),
        corrupted=("is_corrupted", "sum"),
        load_error=("n_load_error", "sum"),
        digest_mismatch=("n_digest_mismatch", "sum"),
        file_sha_mismatch=("n_file_sha_mismatch", "sum"),
    )
    # Rates + 95% CI
    agg["rate"] = agg["corrupted"] / agg["total"]
    cis = agg.apply(lambda r: wilson_ci(int(r["corrupted"]), int(r["total"])), axis=1)
    agg["ci_low"] = [c[0] for c in cis]
    agg["ci_high"] = [c[1] for c in cis]

    # Nice key + ordering
    agg["key"] = agg["mode"] + "|" + agg["write_mode"] + "|" + agg["crash"]
    order_map = {
        "bitflip|atomic|none": 10,
        "zerorange|atomic|none": 20,
        "truncate|atomic|none": 30,
        "none|atomic|none": 40,
        "none|unsafe|early": 50,
        "none|unsafe|mid": 60,
        "none|unsafe|late": 70,
    }
    agg["order"] = agg["key"].map(lambda k: order_map.get(k, 999))
    agg = agg.sort_values(["order", "mode", "write_mode", "crash"]).reset_index(drop=True)
    return agg

def write_table(agg: pd.DataFrame, out_csv="figures/torch_mode_summary.csv", out_md="figures/torch_mode_summary.md") -> None:
    Path(out_csv).parent.mkdir(parents=True, exist_ok=True)
    sel = agg[["mode","write_mode","crash","total","corrupted","rate","ci_low","ci_high","load_error","digest_mismatch","file_sha_mismatch","key"]]
    sel.to_csv(out_csv, index=False)

    # Markdown table for report
    md = ["| mode | write | crash | total | corrupted | rate | 95% CI | load_error | digest_mismatch | file_sha_mismatch |",
          "|:-----|:------|:------|-----:|---------:|-----:|:------:|----------:|----------------:|------------------:|"]
    for _, r in sel.iterrows():
        ci = f"[{r['ci_low']:.3f}, {r['ci_high']:.3f}]"
        md.append(f"| {r['mode']} | {r['write_mode']} | {r['crash']} | {int(r['total'])} | {int(r['corrupted'])} | {r['rate']:.3f} | {ci} | {int(r['load_error'])} | {int(r['digest_mismatch'])} | {int(r['file_sha_mismatch'])} |")
    Path(out_md).write_text("\n".join(md), encoding="utf-8")
    print(f"[table] wrote {out_csv} and {out_md}")

def plot_bars(agg: pd.DataFrame, out_png="figures/torch_mode_bars.png") -> None:
    # One bar per key (mode|write|crash), with 95% CI as error bar
    x = list(agg["key"])
    y = list(agg["rate"])
    yerr_low = [max(0.0, m - lo) for m, lo in zip(y, agg["ci_low"])]
    yerr_high = [max(0.0, hi - m) for m, hi in zip(y, agg["ci_high"])]
    yerr = [yerr_low, yerr_high]

    plt.figure(figsize=(11, 4))
    plt.bar(range(len(x)), y, yerr=yerr, capsize=3)
    plt.xticks(range(len(x)), x, rotation=35, ha="right")
    plt.ylabel("Corruption rate")
    plt.title("Corruption rate by mode|write|crash (95% CI)")
    plt.tight_layout()
    Path(out_png).parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(out_png, dpi=160)
    print(f"[figure] wrote {out_png}")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--runs-root", default="trace/guard/runs_torch")
    ap.add_argument("--out-csv", default="figures/torch_mode_summary.csv")
    ap.add_argument("--out-md", default="figures/torch_mode_summary.md")
    ap.add_argument("--out-png", default="figures/torch_mode_bars.png")
    args = ap.parse_args()

    df = scan_runs(args.runs_root)
    agg = summarize(df)
    write_table(agg, args.out_csv, args.out_md)
    plot_bars(agg, args.out_png)

if __name__ == "__main__":
    main()
