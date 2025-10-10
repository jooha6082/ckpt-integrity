#!/usr/bin/env python3
"""
Build a cross-layer timeline from multiple sources.

Sources
  - iostat-like CSV (timestamped or sample-indexed)
  - checkpoint directory (ckpt/*.pt and *.meta.json)

Unified row schema (when --out is used)
  ts_s,src,name,value,device,extra

Fields
  - ts_s  : float epoch seconds
  - src   : "iostat", "app", or "fs"
  - name  : metric name (e.g., "mbps", "ckpt_write_mb", "detect_ms", "rollback_ms")
  - value : float value
  - device: device label; may be filled with --device-default / --app-device / --fs-device
  - extra : small free-form context (e.g., epoch, tag, source filename)
"""

import argparse
import csv
import json
import re
import sys
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple


# ----------------------------
# Common utilities
# ----------------------------

def to_float(x: str) -> Optional[float]:
    """Parse a float; return None when not parseable."""
    try:
        return float(x)
    except Exception:
        return None


# ----------------------------
# CSV utilities (iostat source)
# ----------------------------

def read_csv_rows(path: Path) -> Tuple[List[str], List[Dict[str, str]]]:
    """Read a CSV file and return (header, rows as dicts)."""
    with path.open("r", newline="") as f:
        sample = f.read(2048)
        f.seek(0)
        try:
            dialect = csv.Sniffer().sniff(sample)
        except Exception:
            dialect = csv.get_dialect("excel")
        reader = csv.DictReader(f, dialect=dialect)
        header = [h.strip() for h in (reader.fieldnames or [])]
        rows = [{k.strip(): (v.strip() if isinstance(v, str) else v) for k, v in row.items()} for row in reader]
    return header, rows


def pick_first(cols: List[str], *candidates: str) -> Optional[str]:
    """Return the first candidate that exists in cols (case-insensitive)."""
    lc = {c.lower(): c for c in cols}
    for cand in candidates:
        if cand.lower() in lc:
            return lc[cand.lower()]
    return None


def detect_layout(header: List[str], sample_override: Optional[str]) -> Dict[str, Optional[str]]:
    """Detect likely column names from variants used earlier."""
    h = header
    return {
        "ts":   pick_first(h, "event_ts", "ts_s", "ts", "timestamp"),
        "samp": sample_override or pick_first(h, "sample"),
        "dev":  pick_first(h, "device", "disk", "diskname", "name"),
        "mbp":  pick_first(h, "io_avg_MBps_pmW", "mbps", "mb_s"),
        "rmb":  pick_first(h, "r_mb_s", "read_mb_s", "rmbs"),
        "wmb":  pick_first(h, "w_mb_s", "write_mb_s", "wmbs"),
        "tps":  pick_first(h, "tps", "io_tps"),
        "tag":  pick_first(h, "mode", "tag"),
    }


def synth_ts(row: Dict[str, str], layout: Dict[str, Optional[str]], base_ts: float, sample_dt: float) -> Optional[float]:
    """Return a timestamp from a row, using ts column if present or sample-index fallback."""
    if layout["ts"]:
        ts_raw = row.get(layout["ts"], "")
        ts = to_float(ts_raw) if ts_raw else None
        if ts is not None:
            return ts
    if layout["samp"]:
        s_raw = row.get(layout["samp"], "")
        s = to_float(s_raw) if s_raw else None
        if s is not None:
            return base_ts + s * sample_dt
    return None


def normalize_iostat(
    header: List[str],
    rows: List[Dict[str, str]],
    *,
    base_ts: float,
    sample_dt: float,
    sample_override: Optional[str],
    device_default: str,
) -> List[Dict[str, str]]:
    """
    Convert raw iostat-like rows into the unified schema.
    Missing columns are skipped; only valid metrics are emitted.
    """
    if not rows:
        return []

    layout = detect_layout(header, sample_override)
    out: List[Dict[str, str]] = []

    for r in rows:
        ts = synth_ts(r, layout, base_ts=base_ts, sample_dt=sample_dt)
        if ts is None:
            continue

        device = r.get(layout["dev"] or "", "") if layout["dev"] else ""
        if not device and device_default:
            device = device_default

        extra = r.get(layout["tag"] or "", "") if layout["tag"] else ""

        def emit(name: str, col: Optional[str]) -> None:
            """Append one metric row if present and parseable."""
            if not col:
                return
            v = to_float(r.get(col, ""))
            if v is None:
                return
            out.append({
                "ts_s": f"{ts:.6f}",
                "src": "iostat",
                "name": name,
                "value": f"{v:.6f}",
                "device": device,
                "extra": extra,
            })

        emit("mbps",  layout["mbp"])
        emit("r_mb_s", layout["rmb"])
        emit("w_mb_s", layout["wmb"])
        emit("tps",   layout["tps"])

    return out


# ----------------------------
# App utilities (checkpoint source)
# ----------------------------

EPOCH_RE = re.compile(r"epoch_(\d+)\.pt$")

def _epoch_from_path(p: Path) -> Optional[int]:
    """Extract epoch number from filenames like epoch_12.pt."""
    m = EPOCH_RE.search(p.name)
    if m:
        try:
            return int(m.group(1))
        except Exception:
            return None
    return None


def scan_ckpt_dir(ckpt_dir: Path, app_device: str = "") -> List[Dict[str, str]]:
    """
    Produce app-level timeline events from a checkpoint directory.

    Emits:
      - ckpt_write_mb : size of *.pt (MB), ts = mtime
      - ckpt_step     : step from *.meta.json, ts = mtime (if present)
    """
    out: List[Dict[str, str]] = []
    if not ckpt_dir.exists():
        return out

    # 1) .pt files → write-size proxy
    for pt in sorted(ckpt_dir.glob("epoch_*.pt")):
        try:
            st = pt.stat()
        except FileNotFoundError:
            continue
        ts = float(st.st_mtime)
        size_mb = st.st_size / (1024.0 * 1024.0)
        epoch = _epoch_from_path(pt)
        extra = f"epoch={epoch}" if epoch is not None else f"path={pt.name}"
        out.append({
            "ts_s": f"{ts:.6f}",
            "src": "app",
            "name": "ckpt_write_mb",
            "value": f"{size_mb:.6f}",
            "device": app_device,
            "extra": extra,
        })

    # 2) .meta.json → step (and short sha) if present
    for meta in sorted(ckpt_dir.glob("epoch_*.pt.meta.json")):
        try:
            st = meta.stat()
            ts = float(st.st_mtime)
            data = json.loads(meta.read_text())
        except Exception:
            continue

        step = data.get("step", None)
        step_f = float(step) if isinstance(step, (int, float)) else None
        sha = data.get("sha256") or data.get("sha") or ""
        epoch = None
        stem = meta.name.replace(".meta.json", "")
        m = EPOCH_RE.search(stem)
        if m:
            try:
                epoch = int(m.group(1))
            except Exception:
                epoch = None

        if step_f is not None:
            bits = []
            if epoch is not None:
                bits.append(f"epoch={epoch}")
            if sha:
                bits.append(f"sha256={str(sha)[:8]}")
            extra = ";".join(bits)
            out.append({
                "ts_s": f"{ts:.6f}",
                "src": "app",
                "name": "ckpt_step",
                "value": f"{step_f:.6f}",
                "device": app_device,
                "extra": extra,
            })

    return out


# ----------------------------
# events summary (fs source)
# ----------------------------

# Accept lines like:
#   detect_ms: 58.1
#   rollback_ms: 2
#   Detect latency (ms)=60.2
#   rollback=3.5 ms
DETECT_RE   = re.compile(r"(?:^|\b)(?:detect|detection)[^0-9\-]*([0-9]+(?:\.[0-9]+)?)\s*ms", re.IGNORECASE)
ROLLBACK_RE = re.compile(r"(?:^|\b)rollback[^0-9\-]*([0-9]+(?:\.[0-9]+)?)\s*ms", re.IGNORECASE)

def parse_events_summary(path: Path) -> List[Tuple[str, float]]:
    """
    Parse detect/rollback latencies (ms) from events_summary.txt.

    Returns a list of (kind, ms) where kind is "detect_ms" or "rollback_ms".
    """
    out: List[Tuple[str, float]] = []
    if not path.exists():
        return out
    for line in path.read_text().splitlines():
        for m in DETECT_RE.finditer(line):
            out.append(("detect_ms", float(m.group(1))))
        for m in ROLLBACK_RE.finditer(line):
            out.append(("rollback_ms", float(m.group(1))))
    return out


def anchor_time_for_fs(
    rows_iostat: List[Dict[str, str]],
    rows_app: List[Dict[str, str]],
    *,
    events_center_ts: Optional[float],
    events_anchor_epoch: Optional[int],
) -> Optional[float]:
    """
    Choose an absolute anchor (epoch seconds) to place fs events on the timeline.

    Priority:
      1) explicit --events-center-ts
      2) first 'ckpt_write_mb' time of --events-anchor-epoch (if provided)
      3) earliest app event time
      4) earliest iostat time
    """
    if events_center_ts is not None:
        return events_center_ts

    if events_anchor_epoch is not None:
        for r in rows_app:
            if r.get("src") == "app" and r.get("name") == "ckpt_write_mb" and f"epoch={events_anchor_epoch}" in r.get("extra", ""):
                t = to_float(r.get("ts_s", ""))
                if t is not None:
                    return t

    # earliest app event
    tmin: Optional[float] = None
    for r in rows_app:
        t = to_float(r.get("ts_s", ""))
        if t is not None:
            tmin = t if tmin is None else min(tmin, t)
    if tmin is not None:
        return tmin

    # earliest iostat event
    for r in rows_iostat:
        t = to_float(r.get("ts_s", ""))
        if t is not None:
            tmin = t if tmin is None else min(tmin, t)
    return tmin


def materialize_fs_events(
    ev_ms: List[Tuple[str, float]],
    anchor_ts: Optional[float],
    fs_device: str,
    source_label: str,
) -> List[Dict[str, str]]:
    """
    Place fs events on the absolute timeline using an anchor time.

    If anchor_ts is None, we cannot place them meaningfully → return empty list.
    """
    out: List[Dict[str, str]] = []
    if anchor_ts is None:
        return out

    for kind, ms in ev_ms:
        ts = anchor_ts + (ms / 1000.0)
        out.append({
            "ts_s": f"{ts:.6f}",
            "src": "fs",
            "name": kind,           # "detect_ms" or "rollback_ms"
            "value": f"{ms:.6f}",   # keep ms as the numeric value
            "device": fs_device,
            "extra": source_label,
        })
    return out


# ----------------------------
# Filtering / summarizing helpers
# ----------------------------

def _time_range(rows: List[Dict[str, str]]) -> Optional[Tuple[float, float]]:
    """Return (min_ts, max_ts) for given rows; None if empty or unparsable."""
    ts_vals: List[float] = []
    for r in rows:
        t = to_float(r.get("ts_s", ""))
        if t is not None:
            ts_vals.append(t)
    if not ts_vals:
        return None
    return (min(ts_vals), max(ts_vals))


def filter_time(rows: List[Dict[str, str]], tmin: Optional[float], tmax: Optional[float]) -> List[Dict[str, str]]:
    """Filter rows to [tmin, tmax]. None means unbounded."""
    out: List[Dict[str, str]] = []
    for r in rows:
        t = to_float(r.get("ts_s", ""))
        if t is None:
            continue
        if (tmin is None or t >= tmin) and (tmax is None or t <= tmax):
            out.append(r)
    return out


def write_csv(path: Path, rows: Iterable[Dict[str, str]]) -> None:
    """Write timeline rows as CSV with a fixed column order."""
    cols = ["ts_s", "src", "name", "value", "device", "extra"]
    with path.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k, "") for k in cols})


def write_table(path: Path, rows: List[Dict[str, str]]) -> None:
    """
    Write a compact summary table grouped by (src, name, device):
      src,name,device,count,min,max,mean
    """
    from math import fsum
    groups: Dict[Tuple[str, str, str], List[float]] = {}
    for r in rows:
        key = (r.get("src", ""), r.get("name", ""), r.get("device", ""))
        v = to_float(r.get("value", ""))
        if v is None:
            continue
        groups.setdefault(key, []).append(v)

    cols = ["src", "name", "device", "count", "min", "max", "mean"]
    with path.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        for (src, name, dev), arr in sorted(groups.items()):
            cnt = len(arr)
            vmin = min(arr)
            vmax = max(arr)
            mean = fsum(arr) / cnt if cnt else 0.0
            w.writerow({
                "src": src, "name": name, "device": dev,
                "count": cnt, "min": f"{vmin:.6f}", "max": f"{vmax:.6f}", "mean": f"{mean:.6f}"
            })


# ----------------------------
# CLI
# ----------------------------

def main() -> int:
    ap = argparse.ArgumentParser(description="Build a cross-layer timeline.")
    # inputs
    ap.add_argument("--iostat",           type=Path, help="input iostat CSV path (timestamped or sample-based)")
    ap.add_argument("--ckpt-dir",         type=Path, help="directory with epoch_*.pt and *.meta.json")
    ap.add_argument("--events-summary",   type=Path, help="summary file with detect/rollback latencies")
    # outputs
    ap.add_argument("--out",              type=Path, help="optional output CSV path (timeline rows)")
    ap.add_argument("--table-out",        type=Path, help="optional output CSV path (summary table)")
    # preview / order
    ap.add_argument("--limit",            type=int, default=10, help="preview limit when --out is not set")
    ap.add_argument("--sort",             action="store_true", help="sort rows by timestamp before output")
    # sample-indexed support
    ap.add_argument("--sample-dt",        type=float, default=1.0, help="seconds per sample when using a sample-indexed CSV")
    ap.add_argument("--base-ts",          type=float, default=0.0, help="epoch seconds to treat sample=0 as")
    ap.add_argument("--sample-col",       type=str, default=None, help="override sample column name if needed")
    # device backfills
    ap.add_argument("--device-default",   type=str, default="", help="label when device column is missing/empty (iostat)")
    ap.add_argument("--app-device",       type=str, default="", help="label for app events")
    ap.add_argument("--fs-device",        type=str, default="fs", help="label for fs events (detect/rollback)")
    # time filtering
    ap.add_argument("--tmin",             type=float, default=None, help="keep events with ts >= tmin (epoch seconds)")
    ap.add_argument("--tmax",             type=float, default=None, help="keep events with ts <= tmax (epoch seconds)")
    ap.add_argument("--overlap-only",     action="store_true", help="keep only the time overlap of provided sources")
    # windowing
    ap.add_argument("--around-epoch",     type=int, default=None, help="center window around ckpt_write_mb of epoch=N")
    ap.add_argument("--window-center-ts", type=float, default=None, help="explicit window center (epoch seconds)")
    ap.add_argument("--window-radius",    type=float, default=None, help="half-width seconds to keep around the center")
    # anchoring FS events
    ap.add_argument("--events-anchor-epoch", type=int, default=None, help="place events_summary latencies relative to ckpt_write_mb of epoch=N")
    ap.add_argument("--events-center-ts",    type=float, default=None, help="explicit anchor time (epoch seconds) for events_summary")
    args = ap.parse_args()

    rows_iostat: List[Dict[str, str]] = []
    rows_app: List[Dict[str, str]] = []
    rows_fs: List[Dict[str, str]] = []

    # iostat
    if args.iostat:
        if not args.iostat.exists():
            print(f"[ERR] not found: {args.iostat}", file=sys.stderr)
            return 2
        header, raw = read_csv_rows(args.iostat)
        rows_iostat = normalize_iostat(
            header, raw,
            base_ts=args.base_ts,
            sample_dt=args.sample_dt,
            sample_override=args.sample_col,
            device_default=args.device_default,
        )

    # app
    if args.ckpt_dir:
        rows_app = scan_ckpt_dir(args.ckpt_dir, app_device=args.app_device)

    # fs (events_summary.txt)
    if args.events_summary:
        ev_ms = parse_events_summary(args.events_summary)
        if ev_ms:
            anchor_ts = anchor_time_for_fs(
                rows_iostat, rows_app,
                events_center_ts=args.events_center_ts,
                events_anchor_epoch=args.events_anchor_epoch,
            )
            rows_fs = materialize_fs_events(
                ev_ms,
                anchor_ts=anchor_ts,
                fs_device=args.fs_device,
                source_label=f"path={args.events_summary.name}",
            )

    # compute overlap window if requested
    tmin = args.tmin
    tmax = args.tmax
    if args.overlap_only:
        ranges = []
        for group in (rows_iostat, rows_app, rows_fs):
            rng = _time_range(group)
            if rng:
                ranges.append(rng)
        if len(ranges) >= 2:
            left = max(r[0] for r in ranges)
            right = min(r[1] for r in ranges)
            if left <= right:
                tmin, tmax = (left, right)

    # optional window centered at a selected event
    if args.window_radius is not None:
        center = args.window_center_ts
        if center is None and args.around_epoch is not None:
            # center at ckpt_write_mb epoch=N if possible
            for r in rows_app:
                if r.get("src") == "app" and r.get("name") == "ckpt_write_mb" and f"epoch={args.around_epoch}" in r.get("extra", ""):
                    t = to_float(r.get("ts_s", ""))
                    if t is not None:
                        center = t
                        break
        if center is not None:
            tmin = center - args.window_radius
            tmax = center + args.window_radius

    # merge and filter
    rows: List[Dict[str, str]] = []
    rows.extend(rows_iostat)
    rows.extend(rows_app)
    rows.extend(rows_fs)
    if tmin is not None or tmax is not None:
        rows = filter_time(rows, tmin, tmax)

    # sort if requested
    if args.sort:
        try:
            rows.sort(key=lambda r: float(r.get("ts_s", "inf")))
        except Exception:
            pass

    # materialize outputs
    if args.out:
        write_csv(args.out, rows)
        print(f"wrote: {args.out} ({len(rows)} rows)")
    if args.table_out:
        write_table(args.table_out, rows)
        print(f"wrote: {args.table_out}")

    # or preview
    if not args.out and not args.table_out:
        total = len(rows)
        print(f"[preview] total rows: {total}")
        for r in rows[: max(0, args.limit)]:
            print(r)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
