#!/usr/bin/env python3
"""
Plot latency histograms from events.csv.

Outputs:
  trace/fig_detect.png   # histogram of detect_ms
  trace/fig_rollback.png # histogram of rollback_ms
"""
import argparse, csv, os
import matplotlib.pyplot as plt  # standard lib on most stacks

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--events", default="ckpt/events.csv")
    p.add_argument("--outdir", default="trace")
    args = p.parse_args()

    detect = []
    rollback = []
    for r in csv.DictReader(open(args.events, newline="")):
        d = r.get("verify_ms")
        rlb = r.get("rollback_ms")
        if d not in (None,""):
            try: detect.append(float(d))
            except: pass
        if rlb not in (None,""):
            try: rollback.append(float(rlb))
            except: pass

    os.makedirs(args.outdir, exist_ok=True)

    # detect histogram
    plt.figure()
    plt.hist(detect, bins=20)
    plt.title("Detect latency (ms)")
    plt.xlabel("ms"); plt.ylabel("count")
    plt.savefig(os.path.join(args.outdir, "fig_detect.png"), dpi=150)

    # rollback histogram
    plt.figure()
    plt.hist(rollback, bins=20)
    plt.title("Rollback time (ms)")
    plt.xlabel("ms"); plt.ylabel("count")
    plt.savefig(os.path.join(args.outdir, "fig_rollback.png"), dpi=150)

    print("saved:", os.path.join(args.outdir, "fig_detect.png"), ",", os.path.join(args.outdir, "fig_rollback.png"))

if __name__ == "__main__":
    main()
