#!/usr/bin/env python3
"""
Table C: fault stats with med/p95 and OK rate (first/final).

Modes
  - window   : first/final은 '에폭 창' 내에서 첫/마지막 이벤트를 비교 (기존과 동일)
  - sequence : 같은 창에서 같은 fault가 연속 발생(시간간격 <= seq_gap_s)이면 한 시퀀스로 묶어
               그 시퀀스의 첫/마지막을 비교 (재시도·회복 분석에 적합)

Inputs
  --timeline: xlayer_timeline.csv
  --events  : ftl_events.csv (event_ts,event,delay_ms,retries)
  --anchor-epoch: 기준 에폭 (t>=0만 사용)

Outputs
  --out-csv: fault,event_count,median_delay_ms,p95_delay_ms,
             ok_rate_first,ok_rate_final,windows_with_event,windows_multi_events,
             sequences,sequences_first_final_diff
  --out-png: 두 패널(OK rates, delays) 그림. 막대 위 값 라벨 표기.

OK definition (configurable):
  delay_ms <= ok_delay_th_ms AND retries <= ok_retries_th
"""

from __future__ import annotations
import argparse, csv, math
from dataclasses import dataclass
from pathlib import Path
from typing import List, Dict, Tuple, Optional

import matplotlib.pyplot as plt


# ------------------ utils ------------------

def f64(s: str) -> Optional[float]:
    try: return float(s)
    except Exception: return None

def i64(s: str) -> int:
    try: return int(float(s))
    except Exception: return 0

def read_csv_rows(p: Path) -> List[Dict[str,str]]:
    if not p.exists(): return []
    rows: List[Dict[str,str]] = []
    with p.open("r", newline="", errors="ignore") as f:
        r = csv.DictReader(f)
        for row in r: rows.append(row)
    return rows

def wcsv(p: Path, rows: List[Dict[str,str]], hdr: List[str]) -> None:
    p.parent.mkdir(parents=True, exist_ok=True)
    with p.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=hdr)
        w.writeheader()
        for r in rows: w.writerow(r)

def percentile(sorted_vals: List[float], p: float) -> float:
    if not sorted_vals: return 0.0
    if p <= 0: return sorted_vals[0]
    if p >= 100: return sorted_vals[-1]
    k = math.ceil(p/100.0 * len(sorted_vals)) - 1
    k = max(0, min(k, len(sorted_vals)-1))
    return sorted_vals[k]


# -------- epoch windows from timeline --------

def epoch_markers(tl: List[Dict[str,str]]) -> List[Tuple[float,str]]:
    out = []
    for r in tl:
        if r.get("src")=="app" and r.get("name")=="ckpt_write_mb":
            t = f64(r.get("ts_s","")); extra = (r.get("extra") or "")
            if t is not None: out.append((t, extra))
    out.sort(key=lambda x:x[0]); return out

def pick_anchor_t0(tl: List[Dict[str,str]], epoch_n: int) -> Optional[float]:
    key=f"epoch={epoch_n}"
    for t,extra in epoch_markers(tl):
        if key in extra: return t
    return epoch_markers(tl)[0][0] if epoch_markers(tl) else None

@dataclass
class Window:
    idx: int
    start_rel_s: float
    end_rel_s: Optional[float]
    start_epoch: str
    end_epoch: str

def build_windows(tl: List[Dict[str,str]], anchor_epoch: int) -> List[Window]:
    marks = epoch_markers(tl)
    if not marks: return []
    t0 = pick_anchor_t0(tl, anchor_epoch)
    if t0 is None: return []

    rel = [(t - t0, extra) for (t,extra) in marks if (t - t0) >= 0.0]
    wins: List[Window] = []
    for i,(st,extra_st) in enumerate(rel):
        en = rel[i+1][0] if i+1 < len(rel) else None
        ee = rel[i+1][1] if i+1 < len(rel) else ""
        wins.append(Window(
            idx=i, start_rel_s=st, end_rel_s=en, start_epoch=extra_st, end_epoch=ee
        ))
    return wins


# ------------------ core ------------------

FAULTS = ("ecc_bypass","ftl_map_corrupt","gc_delay")

def main() -> int:
    ap = argparse.ArgumentParser(description="Build Table C with med/p95 & OK rate (first/final)")
    ap.add_argument("--timeline", type=Path, required=True)
    ap.add_argument("--events", type=Path, required=True)
    ap.add_argument("--anchor-epoch", type=int, default=4)

    # OK rule
    ap.add_argument("--ok-delay-th-ms", type=int, default=0)
    ap.add_argument("--ok-retries-th", type=int, default=0)

    # Grouping mode
    ap.add_argument("--seq-mode", choices=["window","sequence"], default="window",
                    help="window: first/last per window; sequence: group by time gap within window")
    ap.add_argument("--seq-gap-s", type=float, default=120.0,
                    help="max gap (s) to group events into one sequence in sequence-mode")

    ap.add_argument("--out-csv", type=Path, default=Path("trace/xlayer_table_c.csv"))
    ap.add_argument("--out-png", type=Path, default=Path("trace/fig_table_c.png"))
    args = ap.parse_args()

    tl = read_csv_rows(args.timeline)
    ev = read_csv_rows(args.events)
    if not tl or not ev:
        print("[ERR] missing timeline or events"); return 2

    t0 = pick_anchor_t0(tl, args.anchor_epoch)
    if t0 is None:
        print("[ERR] anchor epoch not found"); return 2

    wins = build_windows(tl, args.anchor_epoch)

    # rel events, trim-left
    rel = []
    for r in ev:
        t = f64(r.get("event_ts","")); name = (r.get("event") or "").lower()
        if t is None or not name: continue
        rel_t = t - t0
        if rel_t < 0: continue
        rel.append((rel_t, name, i64(r.get("delay_ms","0")), i64(r.get("retries","0"))))

    # map rel time to window idx
    def win_of(x: float) -> Optional[int]:
        for w in wins:
            if (w.end_rel_s is None and x >= w.start_rel_s) or (x >= w.start_rel_s and x < w.end_rel_s):
                return w.idx
        return None

    # delays per fault (for med/p95)
    delays_by_fault: Dict[str, List[float]] = {f:[] for f in FAULTS}

    # windows_with_event / windows_multi_events
    windows_with_event: Dict[str,int] = {f:0 for f in FAULTS}
    windows_multi_events: Dict[str,int] = {f:0 for f in FAULTS}

    # OK calc source: window units or sequence units
    units_per_fault: Dict[str, List[Tuple[int,List[Tuple[float,int,int]]]]] = {f:[] for f in FAULTS}
    # collect by (win, fault)
    temp: Dict[Tuple[int,str], List[Tuple[float,int,int]]] = {}
    for x, name, dms, rty in rel:
        if name in FAULTS:
            delays_by_fault[name].append(float(dms))
        w = win_of(x)
        if w is None: continue
        k = (w, name)
        temp.setdefault(k, []).append((x, dms, rty))

    for (w, name), rows in temp.items():
        rows.sort(key=lambda t: t[0])
        windows_with_event[name] += 1
        if len(rows) >= 2:
            windows_multi_events[name] += 1

        if args.seq_mode == "window":
            # 한 창 = 하나의 단위
            units_per_fault[name].append((w, rows))
        else:
            # sequence mode: 시간 간격으로 쪼개기
            seqs: List[List[Tuple[float,int,int]]] = []
            cur: List[Tuple[float,int,int]] = []
            for i, tup in enumerate(rows):
                if i == 0:
                    cur = [tup]; continue
                if tup[0] - rows[i-1][0] <= args.seq_gap_s:
                    cur.append(tup)
                else:
                    seqs.append(cur); cur = [tup]
            if cur: seqs.append(cur)
            # 각 시퀀스를 단위로 저장
            for s in seqs:
                units_per_fault[name].append((w, s))

    # OK first/final 계산
    first_ok: Dict[str, List[int]] = {f:[] for f in FAULTS}
    final_ok: Dict[str, List[int]] = {f:[] for f in FAULTS}
    sequences_count: Dict[str,int] = {f:0 for f in FAULTS}
    sequences_diff: Dict[str,int] = {f:0 for f in FAULTS}  # first!=final

    for f in FAULTS:
        for _w, seq in units_per_fault[f]:
            if not seq: continue
            sequences_count[f] += 1
            # first
            _, d1, r1 = seq[0]
            ok1 = (d1 <= args.ok_delay_th_ms and r1 <= args.ok_retries_th)
            first_ok[f].append(1 if ok1 else 0)
            # final
            _, dn, rn = seq[-1]
            okn = (dn <= args.ok_delay_th_ms and rn <= args.ok_retries_th)
            final_ok[f].append(1 if okn else 0)
            if ok1 != okn:
                sequences_diff[f] += 1

    # aggregate rows
    out_rows: List[Dict[str,str]] = []
    for f in FAULTS:
        delays = sorted(delays_by_fault[f])
        med = percentile(delays, 50.0)
        p95 = percentile(delays, 95.0)
        cnt = len(delays)
        denom1 = max(1, len(first_ok[f]))
        denomn = max(1, len(final_ok[f]))
        ok1 = sum(first_ok[f]) / denom1
        okn = sum(final_ok[f]) / denomn
        out_rows.append({
            "fault": f,
            "event_count": str(cnt),
            "median_delay_ms": f"{med:.3f}",
            "p95_delay_ms": f"{p95:.3f}",
            "ok_rate_first": f"{ok1:.3f}",
            "ok_rate_final": f"{okn:.3f}",
            "windows_with_event": str(windows_with_event[f]),
            "windows_multi_events": str(windows_multi_events[f]),
            "sequences": str(sequences_count[f]),
            "sequences_first_final_diff": str(sequences_diff[f]),
        })

    hdr = ["fault","event_count","median_delay_ms","p95_delay_ms",
           "ok_rate_first","ok_rate_final",
           "windows_with_event","windows_multi_events",
           "sequences","sequences_first_final_diff"]
    wcsv(args.out_csv, out_rows, hdr)
    print(f"[OK] wrote {args.out_csv}")

    # figure
    try:
        fig = plt.figure(figsize=(10,4), dpi=150)
        plt.subplots_adjust(bottom=0.25, right=0.98)

        labels = [r['fault'] for r in out_rows]
        ok1s = [float(r["ok_rate_first"]) for r in out_rows]
        okns = [float(r["ok_rate_final"]) for r in out_rows]
        meds = [float(r["median_delay_ms"]) for r in out_rows]
        p95s = [float(r["p95_delay_ms"]) for r in out_rows]
        x = list(range(len(labels))); w = 0.35

        # panel A: OK rates
        ax1 = fig.add_axes([0.07,0.18,0.42,0.74])
        ax1.bar([i-w/2 for i in x], ok1s, width=w, label="OK first", edgecolor="black", linewidth=0.6)
        ax1.bar([i+w/2 for i in x], okns, width=w, label="OK final", edgecolor="black", linewidth=0.6)
        ax1.set_xticks(x); ax1.set_xticklabels(labels)
        ax1.set_ylim(0,1.05); ax1.set_ylabel("OK rate")
        ax1.set_title("OK rate (first vs final)")
        ax1.legend(loc="upper left", framealpha=0.9)
        for xs, ys in [([i-w/2 for i in x], ok1s), ([i+w/2 for i in x], okns)]:
            for X, Y in zip(xs, ys):
                ax1.text(X, Y + 0.02, f"{Y:.2f}", ha="center", va="bottom", fontsize=9)

        # panel B: delays
        ax2 = fig.add_axes([0.55,0.18,0.40,0.74])
        ax2.bar([i-w/2 for i in x], meds, width=w, label="median ms", edgecolor="black", linewidth=0.6)
        ax2.bar([i+w/2 for i in x], p95s, width=w, label="p95 ms", edgecolor="black", linewidth=0.6)
        ax2.set_xticks(x); ax2.set_xticklabels(labels, rotation=12)
        ax2.set_ylabel("delay (ms)")
        ax2.set_title("Delay stats")
        ax2.legend(loc="upper left", framealpha=0.9)
        ymax = max([*p95s, 1.0])
        for xs, ys in [([i-w/2 for i in x], meds), ([i+w/2 for i in x], p95s)]:
            for X, Y in zip(xs, ys):
                ax2.text(X, Y + 0.02*ymax, f"{Y:.0f}", ha="center", va="bottom", fontsize=9)

        args.out_png.parent.mkdir(parents=True, exist_ok=True)
        fig.savefig(args.out_png)
        print(f"[OK] wrote {args.out_png}")
    except Exception as e:
        print(f"[warn] failed to render png: {e}")

    return 0

if __name__ == "__main__":
    raise SystemExit(main())
