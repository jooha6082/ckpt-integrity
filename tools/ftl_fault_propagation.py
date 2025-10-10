#!/usr/bin/env python3
"""
Compute E2E propagation from a fault event to the next app checkpoint (epoch line).

Inputs
  --timeline: xlayer_timeline.csv
  --events:   ftl_events.csv
  --anchor-epoch: start at epoch=N (t>=0)
Outputs
  --out:      per-event CSV with to_next_ckpt_s and window info
  --summary:  per-fault summary stats (count, median, p95, max)
  --out-fig:  optional scatter plot of propagation (s) per event
"""

from __future__ import annotations
import argparse, csv, math
from bisect import bisect_right
from pathlib import Path
from typing import List, Dict, Tuple, Optional
import matplotlib.pyplot as plt

def f64(s: str):
    try: return float(s)
    except Exception: return None

def i64(s: str):
    try: return int(float(s))
    except Exception: return 0

def read_rows(p: Path) -> List[Dict[str,str]]:
    if not p.exists(): return []
    with p.open("r", newline="", errors="ignore") as f:
        r=csv.DictReader(f); return [row for row in r]

def epoch_marks(tl: List[Dict[str,str]]) -> List[Tuple[float,str]]:
    out=[]
    for r in tl:
        if r.get("src")=="app" and r.get("name")=="ckpt_write_mb":
            t=f64(r.get("ts_s","")); extra=(r.get("extra") or "")
            if t is not None: out.append((t,extra))
    out.sort(key=lambda x:x[0]); return out

def pick_anchor(tl, epoch_n: int)->Optional[float]:
    key=f"epoch={epoch_n}"
    for t,extra in epoch_marks(tl):
        if key in extra: return t
    return epoch_marks(tl)[0][0] if epoch_marks(tl) else None

def pct(sorted_vals, p):
    if not sorted_vals: return 0.0
    k = max(0, min(len(sorted_vals)-1, math.ceil(p/100*len(sorted_vals))-1))
    return sorted_vals[k]

def main()->int:
    ap=argparse.ArgumentParser(description="Fault E2E propagation to next app ckpt.")
    ap.add_argument("--timeline", type=Path, required=True)
    ap.add_argument("--events", type=Path, required=True)
    ap.add_argument("--anchor-epoch", type=int, default=4)
    ap.add_argument("--out", type=Path, default=Path("trace/ftl_fault_propagation.csv"))
    ap.add_argument("--summary", type=Path, default=Path("trace/ftl_fault_propagation_summary.csv"))
    ap.add_argument("--out-fig", type=Path, default=Path("trace/fig_fault_propagation.png"))
    args=ap.parse_args()

    tl=read_rows(args.timeline); ev=read_rows(args.events)
    if not tl or not ev:
        print("[ERR] missing inputs"); return 2
    t0=pick_anchor(tl,args.anchor_epoch)
    if t0 is None:
        print("[ERR] anchor not found"); return 2

    # build list of relative epoch boundaries
    lines=[(t-t0,extra) for (t,extra) in epoch_marks(tl) if (t-t0)>=0.0]
    line_x=[x for x,_ in lines]

    # per-event propagation to next epoch line
    out=[]
    for r in ev:
        t=f64(r.get("event_ts","")); evn=(r.get("event") or "").lower()
        if t is None or not evn: continue
        rel=t-t0
        if rel<0: continue
        # next line = right boundary of the window where event falls
        j = bisect_right(line_x, rel)
        if j>=len(line_x):
            nxt=None
        else:
            nxt=line_x[j]
        to_next = (nxt-rel) if nxt is not None else None
        start_epoch = lines[j-1][1] if j-1>=0 else ""
        end_epoch   = lines[j][1]   if j <len(lines) else ""
        out.append({
            "event_ts": f"{t:.6f}",
            "rel_s": f"{rel:.6f}",
            "fault": evn,
            "to_next_ckpt_s": f"{to_next:.6f}" if to_next is not None else "",
            "win_idx": str(j-1 if j-1>=0 else -1),
            "start_epoch": start_epoch,
            "end_epoch": end_epoch,
        })

    # write per-event table
    args.out.parent.mkdir(parents=True, exist_ok=True)
    with args.out.open("w", newline="") as f:
        w=csv.DictWriter(f, fieldnames=["event_ts","rel_s","fault","to_next_ckpt_s","win_idx","start_epoch","end_epoch"])
        w.writeheader()
        for r in out: w.writerow(r)
    print(f"[OK] wrote {args.out}")

    # per-fault summary
    by_fault: Dict[str,List[float]]={}
    for r in out:
        s=r.get("to_next_ckpt_s","")
        if not s: continue
        by_fault.setdefault(r["fault"],[]).append(float(s))
    sum_rows=[]
    for k,v in by_fault.items():
        v.sort()
        sum_rows.append({
            "fault": k,
            "count": str(len(v)),
            "median_to_next_ckpt_s": f"{pct(v,50):.3f}",
            "p95_to_next_ckpt_s": f"{pct(v,95):.3f}",
            "max_to_next_ckpt_s": f"{(v[-1] if v else 0.0):.3f}",
        })
    with args.summary.open("w", newline="") as f:
        w=csv.DictWriter(f, fieldnames=["fault","count","median_to_next_ckpt_s","p95_to_next_ckpt_s","max_to_next_ckpt_s"])
        w.writeheader()
        for r in sum_rows: w.writerow(r)
    print(f"[OK] wrote {args.summary}")

    # plot (optional)
    try:
        fig=plt.figure(figsize=(8,4), dpi=150)
        ax=fig.add_axes([0.10,0.18,0.85,0.76])
        xs=[]; ys=[]; labs=[]
        for i,r in enumerate(out):
            s=r.get("to_next_ckpt_s","")
            if not s: continue
            xs.append(i); ys.append(float(s)); labs.append(r["fault"])
        ax.scatter(xs,ys)
        ax.set_xlabel("event index"); ax.set_ylabel("to next ckpt (s)")
        ax.set_title("Fault propagation to next checkpoint")
        fig.savefig(args.out_fig)
        print(f"[OK] wrote {args.out_fig}")
    except Exception as e:
        print(f"[warn] plot failed: {e}")

    return 0

if __name__=="__main__":
    raise SystemExit(main())
