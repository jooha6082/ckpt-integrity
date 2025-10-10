#!/usr/bin/env python3
"""
Relative cross-layer timeline from a unified CSV.

Schema (from tools/xlayer_timeline.py --out):
  ts_s,src,name,value,device,extra

This variant:
  - Centers time at a chosen checkpoint epoch (default: 4) and trims t<0.
  - Draws app events as vertical lines.
  - Hides 'path' text labels (keeps the event/line itself).
  - Places ONE label per event directly ON the line, positioned relative to
    the reference marker (blue dot). Default is "below" the dot.
  - Blue dots indicate an I/O reference level (mean or max of the iostat series).
  - Optional mean-centered Y so the blue dots sit at the vertical middle.
  - Legend is placed OUTSIDE (right) to avoid overlap.
"""

import argparse
import csv
from pathlib import Path
from typing import Dict, List, Tuple, Optional

import matplotlib.pyplot as plt


# ---------------- I/O utils ----------------

def read_rows(path: Path) -> List[Dict[str, str]]:
    rows: List[Dict[str, str]] = []
    with path.open("r", newline="") as f:
        r = csv.DictReader(f)
        for row in r:
            rows.append(row)
    return rows

def f64(x: str) -> Optional[float]:
    try:
        return float(x)
    except Exception:
        return None


# ---------------- Data extraction ----------------

def series_iostat(rows: List[Dict[str, str]], metric: str) -> Tuple[List[float], List[float]]:
    """Collect (t, v) for src='iostat' and the chosen metric."""
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

def app_events(rows: List[Dict[str, str]], hide_path_label: bool=True) -> List[Tuple[float, str]]:
    """
    Return app events as (timestamp, label).
    Prefer 'extra' (e.g., 'epoch=4'); compress 'path=*' to 'path' and hide if requested.
    """
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
        if hide_path_label and lab == "path":
            lab = ""  # keep line, drop the word
        out.append((t, lab))
    return sorted(out, key=lambda x: x[0])


# ---------------- Time helpers ----------------

def pick_center_epoch(rows: List[Dict[str, str]], epoch_n: int) -> Optional[float]:
    """Find the first ckpt_write_mb whose extra contains epoch=N."""
    key = f"epoch={epoch_n}"
    for r in rows:
        if r.get("src") == "app" and r.get("name") == "ckpt_write_mb":
            if key in (r.get("extra") or ""):
                t = f64(r.get("ts_s", ""))
                if t is not None:
                    return t
    return None

def to_rel(ts: List[float], center: Optional[float]) -> List[float]:
    return [t - center for t in ts] if center is not None else ts

def trim_rel(xs: List[float], ys: List[float],
             tmin: Optional[float], tmax: Optional[float]) -> Tuple[List[float], List[float]]:
    xo, yo = [], []
    for x, y in zip(xs, ys):
        if (tmin is None or x >= tmin) and (tmax is None or x <= tmax):
            xo.append(x); yo.append(y)
    return xo, yo

def trim_events(xs: List[float], meta: List[Tuple[float, str]],
                tmin: Optional[float], tmax: Optional[float]) -> Tuple[List[float], List[Tuple[float, str]]]:
    xo, mo = [], []
    for x, m in zip(xs, meta):
        if (tmin is None or x >= tmin) and (tmax is None or x <= tmax):
            xo.append(x); mo.append(m)
    return xo, mo


# ---------------- Plot ----------------

def plot_rel(rows: List[Dict[str, str]],
             metric: str,
             epoch_center: int,
             trim_left: bool,
             center_mean_y: bool,
             clamp_y_min_zero: bool,
             mark_on_lines: bool,
             mark_y_mode: str,
             label_pos: str,
             y_scale: float,
             width: float, height: float, dpi: int,
             out_path: Optional[Path]) -> None:
    # Data
    ts, vs = series_iostat(rows, metric)
    app_raw = app_events(rows, hide_path_label=True)
    if not ts and not app_raw:
        print("[preview] nothing to plot"); return

    # Center at epoch=N and build relative axis
    center = pick_center_epoch(rows, epoch_center)
    xs = to_rel(ts, center)
    app_xs = to_rel([t for t, _ in app_raw], center)

    # Trim left (drop t<0 to start exactly at the epoch)
    tmin = 0.0 if trim_left else None
    xs, vs = trim_rel(xs, vs, tmin, None)
    app_xs, app_raw = trim_events(app_xs, app_raw, tmin, None)

    # Figure
    fig, ax = plt.subplots(figsize=(width, height), dpi=dpi)
    # leave room on the right for an external legend
    plt.subplots_adjust(right=0.82)

    # Stats AFTER trim
    s_max = max(vs) if vs else 1.0
    s_min = min(vs) if vs else 0.0
    s_mean = (sum(vs) / len(vs)) if vs else 1.0

    # Y-limits
    if center_mean_y and vs:
        half = max(s_mean - s_min, s_max - s_mean) * max(1.0, y_scale)
        y0, y1 = s_mean - half, s_mean + half
        if clamp_y_min_zero and y0 < 0:
            shift = -y0; y0 += shift; y1 += shift
    else:
        y0, y1 = 0.0, s_max * max(1.1, y_scale)

    # Draw iostat series
    if xs:
        ax.plot(xs, vs, marker="o", linewidth=1.6, label=f"{metric} (iostat)")

    # App vertical lines
    for x in app_xs:
        ax.axvline(x, linestyle="--", linewidth=1.1, alpha=0.65)

    # Reference markers (blue dots) on each app line
    if mark_on_lines:
        y_ref = s_max if mark_y_mode == "max" else s_mean
        ax.scatter(app_xs, [y_ref] * len(app_xs), s=30, zorder=3,
                   label=f"ref: {metric} {mark_y_mode}")
    else:
        y_ref = (s_max + s_min) / 2.0  # neutral fallback

    # Labels placed ON the lines relative to the blue dot
    axis_span = max(1e-9, (y1 - y0))
    offset = axis_span * 0.035  # visual gap from the dot
    if label_pos.lower() == "below":
        y_text = y_ref - offset
        va = "top"
    else:  # "above"
        y_text = y_ref + offset
        va = "bottom"

    for x, (_, lab) in zip(app_xs, app_raw):
        if not lab:
            continue  # 'path' hidden
        ax.text(x, y_text, lab, rotation=90, ha="center", va=va, fontsize=9,
                bbox=dict(boxstyle="round,pad=0.12", fc="white", ec="none", alpha=0.85))

    # Style
    ax.set_title("Cross-layer timeline (relative)")
    ax.set_xlabel("time (s) relative")
    ax.set_ylabel(metric)
    ax.set_ylim(y0, y1)
    ax.grid(True, alpha=0.25)

    # Legend outside (right), deduplicated (matches overlay look)
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

    if out_path:
        out_path.parent.mkdir(parents=True, exist_ok=True)
        fig.savefig(out_path)
        print(f"wrote: {out_path}")
    else:
        print(f"[preview] series={len(xs)}, events={len(app_xs)}")


# ---------------- CLI ----------------

def main() -> int:
    ap = argparse.ArgumentParser(description="Relative cross-layer plot: epoch=N start, labels under the dot, 'path' hidden.")
    ap.add_argument("--timeline", type=Path, required=True, help="input CSV from xlayer_timeline.py --out")
    ap.add_argument("--metric", type=str, default="mbps")

    # Center & trim
    ap.add_argument("--center-epoch", type=int, default=4, help="ckpt epoch to use as t=0 (default: 4)")
    ap.add_argument("--trim-left", action="store_true", help="drop t<0 to start at the chosen epoch")

    # Y/markers
    ap.add_argument("--center-mean-y", action="store_true", help="center Y at I/O mean so blue dots sit in the middle")
    ap.add_argument("--clamp-y-min-zero", action="store_true", help="shift up if mean-centered Y dips below 0")
    ap.add_argument("--mark-on-lines", action="store_true", help="place a blue dot on every app vertical")
    ap.add_argument("--mark-y", type=str, choices=["mean","max"], default="mean", help="reference level for blue dots")
    ap.add_argument("--label-pos", type=str, choices=["below","above"], default="below", help="place labels below/above the dot")

    # Figure
    ap.add_argument("--y-scale", type=float, default=1.3, help="axis expansion factor")
    ap.add_argument("--width", type=float, default=12.0)
    ap.add_argument("--height", type=float, default=5.4)
    ap.add_argument("--dpi", type=int, default=150)
    ap.add_argument("--out", type=Path)
    args = ap.parse_args()

    if not args.timeline.exists():
        print(f"[ERR] not found: {args.timeline}"); return 2

    rows = read_rows(args.timeline)
    plot_rel(
        rows,
        metric=args.metric,
        epoch_center=args.center_epoch,
        trim_left=args.trim_left,
        center_mean_y=args.center_mean_y,
        clamp_y_min_zero=args.clamp_y_min_zero,
        mark_on_lines=args.mark_on_lines,
        mark_y_mode=args.mark_y,
        label_pos=args.label_pos,
        y_scale=args.y_scale,
        width=args.width, height=args.height, dpi=args.dpi,
        out_path=args.out,
    )
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
