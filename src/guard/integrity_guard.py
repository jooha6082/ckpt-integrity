#!/usr/bin/env python3
"""
Integrity guard (Step 2)

Adds:
- Schema/shape checks for expected arrays (W1, b1, W2, b2)
- Digest verification: compare loaded arrays' digest vs meta.expected_digest
- Stronger 'corrupted' decision

Output CSV columns (new):
  shape_ok, digest_match, expected_digest_present
"""

from __future__ import annotations
import argparse, csv, hashlib, json, os, re
from typing import Dict, List
import numpy as np


EXPECTED = {
    "W1": {"shape": (128, 128), "dtype": np.float64},
    "b1": {"shape": (128,), "dtype": np.float64},
    "W2": {"shape": (128, 10), "dtype": np.float64},
    "b2": {"shape": (10,), "dtype": np.float64},
}
KEY_ORDER = ["W1", "b1", "W2", "b2"]


def sha256_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()

def array_digest(arr: np.ndarray) -> str:
    h = hashlib.sha256()
    h.update(arr.dtype.str.encode("utf-8"))
    h.update(str(tuple(arr.shape)).encode("utf-8"))
    h.update(arr.tobytes(order="C"))
    return h.hexdigest()

def content_digest(payload: Dict[str, np.ndarray], key_order: List[str]) -> str:
    h = hashlib.sha256()
    for k in key_order:
        d = array_digest(payload[k])
        h.update(k.encode("utf-8")); h.update(d.encode("utf-8"))
    return h.hexdigest()

def parse_epoch_from_name(name: str) -> int:
    m = re.search(r"epoch_(\d+)", name)
    return int(m.group(1)) if m else -1


def scan_dir(ckpt_dir: str, out_csv: str) -> int:
    os.makedirs(os.path.dirname(out_csv), exist_ok=True)
    rows: List[dict] = []

    for fn in sorted(os.listdir(ckpt_dir)):
        if not fn.endswith(".npz"):
            continue
        path = os.path.join(ckpt_dir, fn)
        size = os.path.getsize(path)
        file_sha = sha256_file(path)
        sidecar = path + ".json"

        # Defaults
        load_ok = 1
        nan_total = 0
        inf_total = 0
        note_parts = []

        expected_digest = None
        expected_present = 0

        if os.path.exists(sidecar):
            try:
                meta = json.loads(open(sidecar, "r", encoding="utf-8").read())
                expected_digest = str(meta.get("expected_digest", "") or "")
                if expected_digest:
                    expected_present = 1
            except Exception as e:
                note_parts.append(f"meta_error:{type(e).__name__}")

        # Attempt to load arrays
        arrays: Dict[str, np.ndarray] = {}
        try:
            with np.load(path, allow_pickle=False) as data:
                for k in data.files:
                    arrays[k] = data[k]
                    nan_total += int(np.isnan(arrays[k]).sum())
                    inf_total += int(np.isinf(arrays[k]).sum())
        except Exception as e:
            load_ok = 0
            note_parts.append(f"load_error:{type(e).__name__}")

        # Shape/schema check
        shape_ok = 0
        if load_ok:
            ok = True
            for k, spec in EXPECTED.items():
                if k not in arrays:
                    ok = False; break
                if tuple(arrays[k].shape) != spec["shape"]:
                    ok = False; break
                if arrays[k].dtype != spec["dtype"]:
                    ok = False; break
            shape_ok = 1 if ok else 0
            if not ok:
                note_parts.append("shape_mismatch")

        # Digest verification
        digest_match = 0
        if load_ok and expected_present:
            digest_loaded = content_digest(arrays, KEY_ORDER)
            if digest_loaded == expected_digest:
                digest_match = 1
            else:
                note_parts.append("digest_mismatch")

        # Final corruption decision (strong AND of guards)
        corrupted = int(
            (load_ok == 0) or
            (nan_total > 0) or
            (inf_total > 0) or
            (shape_ok == 0) or
            (expected_present == 1 and digest_match == 0)
        )

        rows.append({
            "epoch": parse_epoch_from_name(fn),
            "file": fn,
            "bytes": size,
            "sha256": file_sha,
            "load_ok": load_ok,
            "nan_total": nan_total,
            "inf_total": inf_total,
            "shape_ok": shape_ok,
            "expected_digest_present": expected_present,
            "digest_match": digest_match,
            "corrupted": corrupted,
            "note": ";".join(note_parts),
        })

    header = ["epoch","file","bytes","sha256","load_ok","nan_total","inf_total",
              "shape_ok","expected_digest_present","digest_match","corrupted","note"]
    with open(out_csv, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=header)
        w.writeheader()
        w.writerows(rows)

    print(f"[guard] wrote {out_csv} ({len(rows)} rows)")
    return len(rows)


def main():
    ap = argparse.ArgumentParser(description="Scan NPZ checkpoints (strong guards)")
    ap.add_argument("--ckpt-dir", default="trace/ckpts")
    ap.add_argument("--out", default="trace/guard/ckpt_scan.csv")
    args = ap.parse_args()
    scan_dir(args.ckpt_dir, args.out)


if __name__ == "__main__":
    main()
