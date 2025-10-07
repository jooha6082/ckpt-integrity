import argparse, os, time, pathlib, shutil, csv, logging
from ckpt_integrity.utils import sha256_file, read_meta

LOG = logging.getLogger("guard")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

def verify_once(path: str):
    meta = read_meta(path)
    if not meta:
        return False, {"reason": "no meta"}
    actual = sha256_file(path)
    ok = (actual == meta.get("sha256"))
    return ok, {"expected": meta.get("sha256"), "actual": actual}

def rollback(dst: str, last_good: str):
    if not os.path.exists(last_good):
        LOG.error("no last-good: %s", last_good)
        return False
    shutil.copy2(last_good, dst)
    return True

def append_event(csv_path: str, row: dict):
    new = not os.path.exists(csv_path)
    with open(csv_path, "a", newline="") as f:
        w = csv.DictWriter(f, fieldnames=row.keys())
        if new:
            w.writeheader()
        w.writerow(row)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--verify", type=str, help="verify a single ckpt file")
    ap.add_argument("--last-good", type=str, default="ckpt/last-good.pt")
    ap.add_argument("--watch", type=str, help="watch directory (*.pt)")
    ap.add_argument("--interval", type=float, default=1.0)
    ap.add_argument("--events", type=str, default="ckpt/events.csv")
    args = ap.parse_args()

    if args.verify:
        t0 = time.time()
        ok, info = verify_once(args.verify)
        took = (time.time() - t0) * 1000.0
        LOG.info("verify: %s -> %s (%.1f ms)", args.verify, ok, took)
        if not ok:
            r0 = time.time()
            if rollback(args.verify, args.last_good):
                LOG.info("rollback: restored from last-good")
                append_event(args.events, {
                    "ts": time.time(), "mode": "single", "path": args.verify,
                    "ok": ok, "verify_ms": round(took,1),
                    "rollback_ms": round((time.time()-r0)*1000.0,1)
                })
        return

    if args.watch:
        LOG.info("watch: %s (%.1fs)", args.watch, args.interval)
        seen = {}
        while True:
            for p in pathlib.Path(args.watch).glob("*.pt"):
                path = str(p)
                mtime = os.path.getmtime(path)
                if seen.get(path) == mtime:
                    continue
                seen[path] = mtime
                t0 = time.time()
                ok, info = verify_once(path)
                took = (time.time() - t0) * 1000.0
                LOG.info("verify: %s -> %s (%.1f ms)", path, ok, took)
                rb_ms = 0.0
                if not ok:
                    r0 = time.time()
                    if rollback(path, args.last_good):
                        rb_ms = (time.time() - r0) * 1000.0
                        LOG.info("rollback: %s (%.1f ms)", path, rb_ms)
                append_event(args.events, {
                    "ts": time.time(), "mode": "watch", "path": path,
                    "ok": ok, "verify_ms": round(took,1),
                    "rollback_ms": round(rb_ms,1)
                })
            time.sleep(args.interval)

if __name__ == "__main__":
    main()
