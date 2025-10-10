#!/usr/bin/env python3
"""
FTL/firmware event pipeline: normalize -> summarize -> overlay plot.

Inputs (choose one; both are optional):
  --events-in  CSV with events (flexible columns)
  --log        Text log to parse (TS + event tokens + optional delay/retries)

Standardized events CSV (always written to --out):
  event_ts,event,delay_ms,retries

Also writes:
  --summary CSV with totals (count, sum_delay_ms, sum_retries)
  --out-plot overlay figure (app epochs + iostat ref + FTL markers)

Design notes
------------
- Anchor time at epoch=N (default 4) and trim-left (drop t<0).
- App events: dashed verticals; label on the line, below the blue dot.
- Blue dots = iostat mean reference level (legend label: "ref: mbps mean").
- Legend is placed **outside** the axes to avoid overlap.
"""

from __future__ import annotations
import argparse
import csv
import re
from pathlib import Path
from typing import Dict, List, Tuple, Optional
from bisect import bisect_right
from collections import defaultdict
import matplotlib.pyplot as plt


# ---------------- tiny utils ----------------

def f64(x: str) -> Optional[float]:
    try:
        return float(x)
    except Exception:
        return None

def i64(x: str) -> int:
    try:
        return int(float(x))
    except Exception:
        return 0

def read_csv_rows(p: Path) -> List[Dict[str, str]]:
    if not p.exists():
        return []
    rows: List[Dict[str, str]] = []
    with p.open("r", newline="", errors="ignore") as f:
        r = csv.DictReader(f)
        for row in r:
            rows.append(row)
    return rows

def write_rows(p: Path, rows: List[Dict[str, str]], fieldnames: List[str]) -> None:
    p.parent.mkdir(parents=True, exist_ok=True)
    with p.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            w.writerow(r)

def series_iostat_in_timeline(rows: List[Dict[str, str]], metric: str="mbps") -> Tuple[List[float], List[float]]:
    """Collect (t, v) from xlayer_timeline rows where src='iostat' and name=metric."""
    ts, vs = [], []
    for r in rows:
        if r.get("src") != "iostat":
            continue
        if r.get("name") != metric:
            continue
        t = f64(r.get("ts_s", "")); v = f64(r.get("value", ""))
        if t is None or v is None:
            continue
        ts.append(t); vs.append(v)
    if not ts:
        return [], []
    tv = sorted(zip(ts, vs), key=lambda x: x[0])
    ts, vs = map(list, zip(*tv))
    return ts, vs



# ---------------- parse simulator text log (optional) ----------------

TS_RE     = re.compile(r"(?P<ts>\[?\d+(?:\.\d+)?\]?)")
EV_RE     = re.compile(r"\b(ecc_bypass|ftl_map_corrupt|gc_delay)\b", re.I)
DELAY_RE  = re.compile(r"(?:delay|lat_ms)\s*[:=]\s*(?P<ms>\d+)\s*ms?", re.I)
RETRY_RE  = re.compile(r"(?:retries?|retry|n_retries)\s*[:=]\s*(?P<n>\d+)", re.I)

def parse_ts(line: str) -> Optional[float]:
    m = TS_RE.search(line)
    if not m:
        return None
    return f64(m.group("ts").strip("[]"))

def parse_ftl_log(log_path: Path) -> List[Dict[str, str]]:
    """Parse plain text lines into the standard event schema."""
    out: List[Dict[str, str]] = []
    with log_path.open("r", errors="ignore") as f:
        for ln in f:
            mm = EV_RE.search(ln)
            if not mm:
                continue
            ts = parse_ts(ln)
            if ts is None:
                continue
            ev = mm.group(1).lower()
            md = DELAY_RE.search(ln); dms = i64(md.group("ms")) if md else 0
            mr = RETRY_RE.search(ln); rty = i64(mr.group("n"))  if mr else 0
            out.append({
                "event_ts": f"{ts:.6f}",
                "event": ev,
                "delay_ms": str(dms),
                "retries": str(rty),
            })
    return out


# ---------------- normalize "any" CSV to the standard schema ----------------

CAND_TS   = ["event_ts","ts","timestamp","time_s","time","t"]
CAND_EV   = ["event","type","name"]
CAND_DMS  = ["delay_ms","delay","d_ms","lat_ms"]
CAND_RTRY = ["retries","retry","n_retries","nretry","n"]

def find_col(cols: List[str], cands: List[str]) -> Optional[str]:
    low = [c.lower() for c in cols]
    for want in cands:
        if want in low:
            return cols[low.index(want)]
    for want in cands:
        for c in cols:
            if want in c.lower():
                return c
    return None

def normalize_events_csv(inp: Path) -> List[Dict[str, str]]:
    rows = read_csv_rows(inp)
    if not rows:
        return []
    cols = list(rows[0].keys())
    c_ts   = find_col(cols, CAND_TS)
    c_ev   = find_col(cols, CAND_EV)
    c_dms  = find_col(cols, CAND_DMS)
    c_rtry = find_col(cols, CAND_RTRY)
    out: List[Dict[str, str]] = []
    for r in rows:
        t  = f64(r.get(c_ts, "")) if c_ts else None
        ev = (r.get(c_ev) or "").lower() if c_ev else ""
        if t is None or not ev:
            continue
        dms = i64(r.get(c_dms, "0")) if c_dms else 0
        rty = i64(r.get(c_rtry, "0")) if c_rtry else 0
        out.append({
            "event_ts": f"{t:.6f}",
            "event": ev,
            "delay_ms": str(dms),
            "retries": str(rty),
        })
    return out


# ---------------- overall summary ----------------

def build_summary(rows: List[Dict[str, str]]) -> List[Tuple[str, int, int, int]]:
    agg: Dict[str, Tuple[int, int, int]] = {}
    for r in rows:
        ev = (r.get("event") or "").lower()
        if not ev:
            continue
        c, d, rt = agg.get(ev, (0, 0, 0))
        agg[ev] = (c + 1,
                   d + i64(r.get("delay_ms", "0")),
                   rt + i64(r.get("retries", "0")))
    ordered: List[Tuple[str, int, int, int]] = []
    for ev in ("ecc_bypass", "ftl_map_corrupt", "gc_delay"):
        c, d, rt = agg.get(ev, (0, 0, 0))
        ordered.append((ev, c, d, rt))
    for ev in sorted(k for k in agg if k not in {"ecc_bypass", "ftl_map_corrupt", "gc_delay"}):
        c, d, rt = agg[ev]
        ordered.append((ev, c, d, rt))
    return ordered

def write_summary(rows: List[Tuple[str, int, int, int]], out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["event", "count", "sum_delay_ms", "sum_retries"])
        for r in rows:
            w.writerow(list(r))
    print(f"[OK] summary -> {out_path}")


# ---------------- timeline helpers ----------------

def timeline_app_events(rows: List[Dict[str, str]], hide_path: bool=True) -> List[Tuple[float, str]]:
    out: List[Tuple[float, str]] = []
    for r in rows:
        if r.get("src") != "app":
            continue
        t = f64(r.get("ts_s", ""))
        if t is None:
            continue
        lab = (r.get("extra") or r.get("name") or "").strip()
        if lab.startswith("path="):
            lab = "path"
        if hide_path and lab == "path":
            lab = ""
        out.append((t, lab))
    out.sort(key=lambda x: x[0])
    return out

def pick_anchor_t0(rows: List[Dict[str, str]], epoch_n: int) -> Optional[float]:
    key = f"epoch={epoch_n}"
    for r in rows:
        if r.get("src") == "app" and r.get("name") == "ckpt_write_mb" and key in (r.get("extra") or ""):
            t = f64(r.get("ts_s", ""))
            if t is not None:
                return t
    # fallback: earliest app event
    apps = [f64(r.get("ts_s", "")) for r in rows if r.get("src") == "app"]
    apps = [t for t in apps if t is not None]
    return min(apps) if apps else None

def io_mean_from_event_io(io_csv: Path) -> float:
    rows = read_csv_rows(io_csv)
    if not rows:
        return 1.0
    # try to find an "mbps" column
    cols = list(rows[0].keys())
    cand = None
    for k in cols:
        if k.lower() in ("io_avg_mbps_pmw", "mbps", "io_mbps", "io_avg_mbps"):
            cand = k; break
    if cand is None:
        for k in cols:
            if "mbps" in k.lower():
                cand = k; break
    if cand is None:
        return 1.0
    vals = [f64(r.get(cand, "")) for r in rows]
    vals = [v for v in vals if v is not None]
    return (sum(vals) / len(vals)) if vals else 1.0

def to_rel(ts_abs: List[float], t0: Optional[float]) -> List[float]:
    return [t - t0 for t in ts_abs] if t0 is not None else ts_abs


# ---------------- overlay plot ----------------
def overlay_plot(timeline_csv: Path, io_csv: Path, ftl_csv: Path,
                 anchor_epoch: int, out_png: Path) -> None:
    # Load timeline rows (per-event timeline with ts_s)
    tl = read_csv_rows(timeline_csv)
    if not tl or "ts_s" not in tl[0]:
        print(f"[warn] {timeline_csv} is not a per-event timeline (needs ts_s). Skip plot.")
        return

    # App events (vertical lines)
    apps = timeline_app_events(tl, hide_path=True)
    if not apps:
        print("[warn] no app events on timeline; skip plot.")
        return

    # Anchor & trim-left
    t0 = pick_anchor_t0(tl, anchor_epoch)
    app_x_abs = [t for t, _ in apps]
    app_x_rel = to_rel(app_x_abs, t0)
    app_rel = [(x, apps[i][1]) for i, x in enumerate(app_x_rel) if x >= 0.0]

    # Reference level: iostat mean from TIMELINE (unified with xlayer_plot)
    def series_iostat_in_timeline(rows: List[Dict[str, str]], metric: str="mbps"):
        ts, vs = [], []
        for r in rows:
            if r.get("src") != "iostat": continue
            if r.get("name") != metric:   continue
            t = f64(r.get("ts_s","")); v = f64(r.get("value",""))
            if t is None or v is None:    continue
            ts.append(t); vs.append(v)
        if not ts: return [], []
        tv = sorted(zip(ts, vs), key=lambda x: x[0])
        ts, vs = map(list, zip(*tv))
        return ts, vs

    ts_io, vs_io = series_iostat_in_timeline(tl, "mbps")
    xs_io_rel = to_rel(ts_io, t0)
    vs_trim = [v for x, v in zip(xs_io_rel, vs_io) if x >= 0.0]
    y_mean = (sum(vs_trim) / len(vs_trim)) if vs_trim else 1.0

    # Read FTL events and convert to relative time
    ftl_rows = read_csv_rows(ftl_csv)
    ftl_rel_raw = []
    for r in ftl_rows:
        t = f64(r.get("event_ts","")); ev = (r.get("event") or "").lower()
        if t is None or not ev:
            continue
        rel = t - t0 if t0 is not None else t
        if rel >= 0.0:
            ftl_rel_raw.append((rel, ev))

    # ----- SNAP: 이벤트를 "가장 가까운 왼쪽 에폭선"으로 정렬 (x는 선에 맞춤) -----
    # 목적: 마커들이 선에서 삐져나오지 않게 한 줄에 세움.
    # 겹칠 경우 y만 소량씩 위/아래로 벌려서 구분.
    app_x_sorted = sorted([x for x, _ in app_rel])  # relative positions of epoch lines
    def snap_to_epoch_line(x: float) -> Optional[float]:
        if not app_x_sorted:
            return None
        idx = bisect_right(app_x_sorted, x) - 1  # window start (left edge)
        if idx < 0:
            return None
        return app_x_sorted[idx]

    # figure
    fig, ax = plt.subplots(figsize=(12, 5.4), dpi=150)
    plt.subplots_adjust(right=0.82)  # leave space for external legend

    # Draw epoch verticals
    for x, _lab in app_rel:
        ax.axvline(x, linestyle="--", linewidth=1.1, alpha=0.65, color="#1f77b4")

    # Blue reference dots at the mean (explained in legend)
    if app_rel:
        ax.scatter([x for x, _ in app_rel], [y_mean] * len(app_rel),
                   s=30, color="#1f77b4", label="ref: mbps mean")
        # Labels ON the lines, below the blue dot
        offset = 0.03 * max(y_mean, 1.0)
        for x, lab in app_rel:
            if not lab: continue
            ax.text(x, y_mean - offset, lab, rotation=90,
                    ha="center", va="top", fontsize=9,
                    bbox=dict(boxstyle="round,pad=0.12", fc="white", ec="none", alpha=0.85))

    # FTL markers: same Y line, vertical-only separation if overlapping
    palette = {"ecc_bypass": "#1f77b4", "ftl_map_corrupt": "#ff7f0e", "gc_delay": "#2ca02c"}
    style   = {
        "ecc_bypass":      dict(marker="^", s=55, color=palette["ecc_bypass"],      label="ecc_bypass"),
        "ftl_map_corrupt": dict(marker="x", s=70, color=palette["ftl_map_corrupt"], label="ftl_map_corrupt"),
        "gc_delay":        dict(marker="s", s=50, color=palette["gc_delay"],        label="gc_delay"),
    }

    y_base = y_mean * 1.04 if y_mean > 0 else 0.04   # one common line slightly above the blue dots
    dy = max(0.012 * max(y_mean, 1.0), 0.01)         # vertical separation step

    # per-epoch-line stacking state (to alternate up/down: 0, +1, -1, +2, -2, ...)
    stack_counter = defaultdict(int)
    def next_offset(n: int) -> int:
        if n == 0: return 0
        k = (n + 1) // 2
        return +k if n % 2 == 1 else -k

    # plot in fixed order for stable legend
    for ev in ("ecc_bypass", "ftl_map_corrupt", "gc_delay"):
        xs_snap, ys = [], []
        for (t_rel, e) in ftl_rel_raw:
            if e != ev: continue
            sx = snap_to_epoch_line(t_rel)
            if sx is None: continue
            k = stack_counter[(sx,)]  # single key per line
            stack_counter[(sx,)] = k + 1
            y = y_base + next_offset(k) * dy
            xs_snap.append(sx); ys.append(y)
        if xs_snap:
            ax.scatter(xs_snap, ys, **style[ev])

    # Axes & look
    ax.set_title("Cross-layer timeline (relative)")
    ax.set_xlabel("time (s) relative")
    ax.set_ylabel("mbps")
    ax.set_ylim(0.0, (y_mean * 1.27) if y_mean > 0 else 1.27)
    ax.grid(True, alpha=0.25)

    # Legend outside (right), deduplicated
    handles, labels = ax.get_legend_handles_labels()
    by_label = dict(zip(labels, handles))
    ax.legend(
        by_label.values(), by_label.keys(),
        loc="upper left",
        bbox_to_anchor=(1.01, 1.0),
        borderaxespad=0.0,
        framealpha=0.9,
        title="Markers",
    )

    fig.tight_layout()
    out_png.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out_png)
    print(f"[OK] overlay plot -> {out_png}")



# ---------------- orchestrate ----------------

def ensure_events_csv(log_path: Optional[Path], events_in: Optional[Path], out_csv: Path) -> List[Dict[str, str]]:
    """
    Priority:
      1) --events-in given and exists -> normalize & write --out
      2) else if --log exists        -> parse & write --out
      3) else if --out exists        -> reuse
      4) else                        -> create empty header
    """
    if events_in and events_in.exists():
        rows = normalize_events_csv(events_in)
        write_rows(out_csv, rows, ["event_ts", "event", "delay_ms", "retries"])
        print(f"[OK] normalized events from {events_in} -> {out_csv} ({len(rows)} rows)")
        return rows
    if log_path and log_path.exists():
        rows = parse_ftl_log(log_path)
        write_rows(out_csv, rows, ["event_ts", "event", "delay_ms", "retries"])
        print(f"[OK] parsed {len(rows)} events from {log_path} -> {out_csv}")
        return rows
    if out_csv.exists():
        rows = read_csv_rows(out_csv)
        print(f"[OK] reuse existing events: {out_csv} ({len(rows)} rows)")
        return rows
    write_rows(out_csv, [], ["event_ts", "event", "delay_ms", "retries"])
    print(f"[warn] no events provided; created empty {out_csv}")
    return []


# ---------------- CLI ----------------

def main() -> int:
    ap = argparse.ArgumentParser(description="FTL events -> CSV/summary/overlay.")
    ap.add_argument("--log", type=Path, help="Simulator text log to parse")
    ap.add_argument("--events-in", type=Path, help="Existing CSV with events (any reasonable columns)")
    ap.add_argument("--out", type=Path, default=Path("trace/ftl_events.csv"))
    ap.add_argument("--summary", type=Path, default=Path("trace/xlayer_event_summary.csv"))
    ap.add_argument("--timeline", type=Path, default=Path("trace/xlayer_timeline.csv"))
    ap.add_argument("--io", type=Path, default=Path("trace/event_io.csv"))
    ap.add_argument("--anchor-epoch", type=int, default=4)
    ap.add_argument("--out-plot", type=Path, default=Path("trace/fig_xlayer_relative_overlay.png"))
    ap.add_argument("--no-plot", action="store_true")
    args = ap.parse_args()

    rows = ensure_events_csv(args.log, args.events_in, args.out)
    write_summary(build_summary(rows), args.summary)

    if not args.no_plot:
        overlay_plot(args.timeline, args.io, args.out, args.anchor_epoch, args.out_plot)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
