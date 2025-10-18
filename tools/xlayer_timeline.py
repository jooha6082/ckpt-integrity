#!/usr/bin/env python3
"""
Build a simple cross-layer timeline CSV from:
- app log (prefixed with epoch seconds by run_with_trace)
- fs_usage (macOS)
- iostat

Output CSV schema:
  ts_s,src,name,value,device,extra

This is a minimal v1 parser aimed at being robust rather than perfect.
"""
from __future__ import annotations
import argparse, re, time, datetime as dt
from pathlib import Path

def parse_app_log(path: Path):
    # lines: "<epoch_ts> APP_EVENT,checkpoint_saved,ts=...,epoch=E,path=..."
    rows=[]
    for line in path.read_text(errors="ignore").splitlines():
        line=line.strip()
        if not line: continue
        try:
            sp=line.split(" ",1)
            ts=float(sp[0]); payload=sp[1]
        except Exception:
            continue
        if payload.startswith("APP_EVENT,"):
            fields = dict()
            parts = payload.strip().split(",")
            name = parts[1] if len(parts)>1 else "event"
            for p in parts[2:]:
                if "=" in p:
                    k,v=p.split("=",1); fields[k]=v
            epoch = fields.get("epoch","")
            rows.append({
                "ts_s": ts,
                "src": "app",
                "name": name,
                "value": epoch,
                "device": "",
                "extra": fields.get("path","")
            })
    return rows

def parse_fs_usage(path: Path, today: dt.date):
    # typical line starts with "HH:MM:SS.uuuuuu " ; we map to today's date
    rows=[]
    pat = re.compile(r"^(\d{2}):(\d{2}):(\d{2})\.(\d{6})\s+(.*)$")
    for line in path.read_text(errors="ignore").splitlines():
        m=pat.match(line)
        if not m: 
            continue
        h,mn,s,us,rest = m.groups()
        tm = dt.datetime(today.year,today.month,today.day,int(h),int(mn),int(s),int(us))
        ts = tm.timestamp()
        # try to extract operation and path
        # rest examples vary; keep it raw
        rows.append({
            "ts_s": ts,
            "src": "fs_usage",
            "name": "fs_op",
            "value": "",
            "device": "",
            "extra": rest
        })
    return rows

def parse_iostat(path: Path, start_guess: float):
    # Parse per-interval device lines, derive a simple tps number
    rows=[]
    for line in path.read_text(errors="ignore").splitlines():
        line=line.strip()
        if not line: continue
        # skip header-ish lines
        if any(k in line.lower() for k in ["cpu", "disk", "kb", "tps", "device", "load average"]):
            continue
        parts=line.split()
        # macOS iostat tends to emit: disk0   KB/t tps  MB/s
        # We try to capture "tps" as the second column if numeric
        nums=[p for p in parts if re.match(r"^-?\d+(\.\d+)?$", p)]
        if not nums: 
            continue
        try:
            tps=float(nums[1]) if len(nums)>1 else float(nums[0])
        except Exception:
            continue
        start_guess += 1.0
        rows.append({
            "ts_s": start_guess,
            "src": "iostat",
            "name": "tps",
            "value": f"{tps}",
            "device": "",
            "extra": "iostat_line"
        })
    return rows

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--app-log", required=True, help="trace/sys/run_*.applog")
    ap.add_argument("--fs-usage", required=True, help="trace/sys/run_*.fs_usage")
    ap.add_argument("--iostat", required=True, help="trace/sys/run_*.iostat")
    ap.add_argument("--out", default="trace/timeline/timeline.csv")
    args = ap.parse_args()

    p_app=Path(args.app_log); p_fsu=Path(args.fs_usage); p_ios=Path(args.iostat)
    out=Path(args.out); out.parent.mkdir(parents=True, exist_ok=True)

    # app log gives us an absolute epoch reference
    app_rows = parse_app_log(p_app)
    start_ts = min(r["ts_s"] for r in app_rows) if app_rows else time.time()
    today = dt.date.today()

    fs_rows = parse_fs_usage(p_fsu, today)
    io_rows = parse_iostat(p_ios, start_ts)

    rows = app_rows + fs_rows + io_rows
    rows.sort(key=lambda r: r["ts_s"])

    with open(out, "w") as f:
        f.write("ts_s,src,name,value,device,extra\n")
        for r in rows:
            f.write(f'{r["ts_s"]:.6f},{r["src"]},{r["name"]},{r["value"]},{r["device"]},"{r["extra"].replace("\"","\'")}"\n')
    print(f"[timeline] wrote {out} ({len(rows)} rows)")

if __name__ == "__main__":
    main()
