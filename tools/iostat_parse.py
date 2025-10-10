import argparse, csv, re, sys
from typing import List, Optional

def looks_linux_header(s: str) -> bool:
    return s.startswith("Device:") and ("kB_read/s" in s or "rkB/s" in s)

def looks_mac_names(s: str) -> List[str]:
    # ex) "          disk0           disk1"
    toks = s.split()
    return [t for t in toks if t.startswith("disk")]

def looks_mac_cols(s: str) -> bool:
    # ex) "KB/t  tps  MB/s    KB/t  tps  MB/s"
    return ("KB/t" in s) and ("tps" in s) and ("MB/s" in s)

def parse_mac_values_line(s: str, ndev: int) -> Optional[List[float]]:
    # ex) "23.36  122  2.80   10.7   5  0.05" → 6 numbers for 2 devs
    nums = re.findall(r"[+-]?\d+(?:\.\d+)?", s)
    if len(nums) < 3 * ndev:
        return None
    try:
        return list(map(float, nums[:3*ndev]))
    except ValueError:
        return None

def parse_linux_values_line(s: str) -> Optional[tuple]:
    # ex) "sda  120.0  1024.0  512.0 ..." → we take tps, kB_read/s, kB_wrtn/s if present
    parts = s.split()
    if len(parts) < 4 or parts[0] == "avg-cpu:":
        return None
    dev = parts[0]
    try:
        tps = float(parts[1])
        rb = float(parts[2])
        wb = float(parts[3])
    except ValueError:
        return None
    return dev, tps, rb, wb

def main():
    ap = argparse.ArgumentParser(description="Parse iostat log into CSV")
    ap.add_argument("-i", "--input", required=True, help="path to iostat log (trace/iostat_*.log)")
    ap.add_argument("-o", "--output", required=True, help="CSV output path")
    ap.add_argument("--per-device", action="store_true", help="emit per-device rows (default: yes)")
    args = ap.parse_args()
    args.per_device = True  # always per-device for now (simpler)

    with open(args.input, "r", errors="ignore") as f, open(args.output, "w", newline="") as out:
        w = csv.writer(out)
        w.writerow(["sample", "os", "device", "tps", "mb_s", "kb_read_s", "kb_wrtn_s"])

        sample = 0
        os_mode = None  # "mac" or "linux"
        mac_devices: List[str] = []
        ready_for_values = False

        for raw in f:
            line = raw.strip()
            if not line:
                continue

            # Detect linux header
            if looks_linux_header(line):
                os_mode = "linux"
                continue

            # Detect mac names/cols
            names = looks_mac_names(line)
            if names:
                os_mode = "mac"
                mac_devices = names
                ready_for_values = False
                continue
            if os_mode == "mac" and looks_mac_cols(line):
                ready_for_values = True
                continue

            if os_mode == "mac" and ready_for_values and mac_devices:
                vals = parse_mac_values_line(line, len(mac_devices))
                if not vals:
                    continue
                # vals is [KB/t0, tps0, MB/s0, KB/t1, tps1, MB/s1, ...]
                for idx, dev in enumerate(mac_devices):
                    base = idx * 3
                    tps = vals[base + 1]
                    mb_s = vals[base + 2]
                    w.writerow([sample, "mac", dev, tps, mb_s, "", ""])
                sample += 1
                continue

            if os_mode == "linux":
                parsed = parse_linux_values_line(line)
                if parsed:
                    dev, tps, rb, wb = parsed
                    # Approx MB/s from kB/s
                    mb_s = (rb + wb) / 1024.0
                    w.writerow([sample, "linux", dev, tps, f"{mb_s:.4f}", f"{rb:.4f}", f"{wb:.4f}"])
                    sample += 1

    print(f"OK -> {args.output}")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(130)