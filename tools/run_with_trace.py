#!/usr/bin/env python3
# Run ckpt_writer while capturing fs_usage/iostat, teeing app logs with timestamps.
from __future__ import annotations
import argparse, subprocess, sys, time
from pathlib import Path
from src.traceutils.fs_trace import spawn_fs_usage, spawn_iostat

def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--epochs", type=int, default=60)
    ap.add_argument("--every", type=int, default=3)
    ap.add_argument("--seed", type=int, default=0)
    ap.add_argument("--fault", default="none")
    ap.add_argument("--write-mode", default="atomic", choices=["atomic","unsafe"])
    ap.add_argument("--crash-when", default="none", choices=["none","early","mid","late"])
    ap.add_argument("--out", default="trace/ckpts_traced/run_0")
    ap.add_argument("--iostat-interval", type=int, default=1)
    ap.add_argument("--pause-ms", type=int, default=0)
    args = ap.parse_args()

    Path(args.out).mkdir(parents=True, exist_ok=True)
    sys_dir = Path("trace/sys"); sys_dir.mkdir(parents=True, exist_ok=True)

    # choose crash epoch (match tools/run_many.py logic)
    def ckpt_epochs(epochs, every): return list(range(every, epochs+1, every))
    crash_epoch = -1
    cks = ckpt_epochs(args.epochs, args.every)
    if args.crash_when == "early" and cks: crash_epoch = cks[0]
    if args.crash_when == "mid" and cks:   crash_epoch = cks[len(cks)//2]
    if args.crash_when == "late" and cks:  crash_epoch = cks[-1]

    run_tag = f"{args.fault}__{args.write_mode}__{args.crash_when}__seed{args.seed}__{int(time.time())}"
    app_log = sys_dir / f"run_{run_tag}.applog"
    fsu_log = sys_dir / f"run_{run_tag}.fs_usage"
    ios_log = sys_dir / f"run_{run_tag}.iostat"

    # spawn tracers
    fsu = spawn_fs_usage(str(fsu_log))
    ios = spawn_iostat(str(ios_log), interval_sec=args.iostat_interval)

    # launch app and tee stdout with epoch timestamp
    cmd = [
        sys.executable, "-m", "src.aiwork.ckpt_writer",
        "--epochs", str(args.epochs), "--checkpoint-every", str(args.every),
        "--out", str(args.out), "--seed", str(args.seed),
        "--fault", args.fault, "--write-mode", args.write_mode,
        "--pause-ms", str(args.pause_ms),
    ]
    if crash_epoch > 0: cmd += ["--crash-epoch", str(crash_epoch)]
    print("[run]", " ".join(cmd))
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    with open(app_log, "w") as f:
        for line in p.stdout:
            ts = time.time()
            f.write(f"{ts:.6f} {line}")
            f.flush()
            print(line, end="")  # also mirror to console

    rc = p.wait()

    # stop tracers
    fsu.stop(); ios.stop()
    print(f"[trace] app rc={rc}; logs: {app_log.name}, {fsu_log.name}, {ios_log.name}")

if __name__ == "__main__":
    main()
