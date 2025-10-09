#!/usr/bin/env python3
"""
Update <epoch>.meta.json['sha256'] to match the current <epoch>.pt.
Writes atomically to avoid torn metadata.
"""
import argparse, json, hashlib, os, tempfile, shutil

def sha256(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for b in iter(lambda: f.read(1<<20), b""):
            h.update(b)
    return h.hexdigest()

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("ckpt_pt", help="path to checkpoint .pt file")
    ap.add_argument("--meta", help="path to meta json (default: same stem + .meta.json)")
    args = ap.parse_args()

    pt = args.ckpt_pt
    meta = args.meta or os.path.splitext(pt)[0] + ".meta.json"
    if not os.path.exists(pt): raise SystemExit(f"missing {pt}")
    if not os.path.exists(meta): raise SystemExit(f"missing {meta}")

    new = sha256(pt)
    obj = json.load(open(meta))
    obj["sha256"] = new

    d = os.path.dirname(meta) or "."
    fd, tmp = tempfile.mkstemp(prefix=".meta.", dir=d); os.close(fd)
    with open(tmp, "w") as w:
        json.dump(obj, w, indent=2); w.write("\n")
    shutil.move(tmp, meta)
    print(f"meta updated: {meta} -> {new}")

if __name__ == "__main__":
    main()
