#!/usr/bin/env python3
# Run group checkpoint fuzz: seeds × crash-points, then aggregate & summarize.
from __future__ import annotations
import argparse, subprocess, sys, re
from pathlib import Path
import pandas as pd

def parse_range(s: str) -> list[int]:
    out=[]
    for tok in s.split(","):
        tok=tok.strip()
        if not tok: continue
        if "-" in tok:
            a,b=tok.split("-",1); a=int(a); b=int(b)
            lo,hi=min(a,b),max(a,b)
            out.extend(range(lo,hi+1))
        else:
            out.append(int(tok))
    return sorted(set(out))

def run(cmd: list[str], check=True):
    print("[run]", " ".join(cmd))
    subprocess.run(cmd, check=check)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--epochs", type=int, default=12)
    ap.add_argument("--every", type=int, default=3)
    ap.add_argument("--seeds", default="0-9")
    ap.add_argument("--root", default="trace/groups/fuzz")
    ap.add_argument("--agg-out", default="trace/guard/group_scan_all.csv")
    ap.add_argument("--pause-ms", type=int, default=0)
    ap.add_argument("--kb-model", type=int, default=128)
    ap.add_argument("--kb-optim", type=int, default=64)
    args = ap.parse_args()

    seeds = parse_range(args.seeds)
    py = sys.executable

    roots = []

    # 1) Atomic (golden): expect all group_ok=1
    for s in seeds:
        r = Path(args.root) / f"atomic_seed{s}"
        roots.append((str(r), "atomic", s, "none"))
    run([py,"-m","src.aiwork.group_ckpt",
        "--out", str(r),
        "--epochs", str(args.epochs), "--every", str(args.every),
        "--write-mode", "atomic", "--seed", str(s), "--fault", "none",
        "--kb-model", str(args.kb_model), "--kb-optim", str(args.kb_optim),
        "--pause-ms", str(args.pause_ms)])


    # 2) Unsafe + crash points (one in-flight epoch should fail per seed)
    crash_points = ["after_model","before_manifest","manifest_partial","before_commit"]
    for cp in crash_points:
        for s in seeds:
            r = Path(args.root) / f"unsafe_{cp}_seed{s}"
            roots.append((str(r), "unsafe", s, cp))
            run([py,"-m","src.aiwork.group_ckpt",
                 "--out", str(r), "--epochs", str(args.epochs), "--every", str(args.every),
                 "--write-mode", "unsafe", "--seed", str(s), "--fault", "none",
                 "--crash-at", cp], check=False)   # may exit early by design

    # 3) Scan each root and tag metadata
    per_csv = []
    for (root_path, write_mode, seed, crash_at) in roots:
        out_csv = Path("trace/guard") / "group_scans" / f"{Path(root_path).name}.csv"
        out_csv.parent.mkdir(parents=True, exist_ok=True)
        run([py,"-m","src.guard.group_guard","--root", root_path, "--out", str(out_csv)])
        if Path(out_csv).exists():
            df = pd.read_csv(out_csv)
            df = df.assign(root=root_path, write_mode=write_mode, seed=seed, crash_at=crash_at)
            per_csv.append(df)

    if not per_csv:
        print("[agg] no scans found")
        return

    agg = pd.concat(per_csv, ignore_index=True)
    Path(args.agg_out).parent.mkdir(parents=True, exist_ok=True)
    agg.to_csv(args.agg_out, index=False)
    print(f"[agg] wrote {args.agg_out} ({len(agg)} rows)")

    # 4) Quick summary to stdout
    def summarize(df: pd.DataFrame, title: str):
        g = df.groupby(["write_mode","crash_at"], as_index=False).agg(
            total=("group_ok","size"),
            ok=("group_ok","sum")
        )
        print(f"\n[summary] {title}")
        for _,r in g.iterrows():
            rate = (r["ok"]/r["total"]) if r["total"] else 0.0
            print(f"  write={r['write_mode']:<6} crash={r['crash_at']:<16} : group_ok={int(r['ok'])}/{int(r['total'])} ({rate:.3f})")

    summarize(agg, "group_ok by write/crash")

if __name__ == "__main__":
    main()
