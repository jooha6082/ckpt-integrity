#!/usr/bin/env python3
"""
Integrity scanner for Torch .pt checkpoints with:
- loadability + NaN/Inf checks
- tensor schema/shape checks
- content digest verification (expected_digest)
- file-level hash verification (expected_file_sha256)  ← NEW

Output CSV:
  epoch,file,bytes,sha256,load_ok,nan_total,inf_total,shape_ok,
  expected_digest_present,digest_match,
  expected_file_sha_present,file_sha_match,   ← NEW
  corrupted,note
"""
from __future__ import annotations
import argparse, csv, hashlib, json, os, re
from typing import Dict, List
import torch
import numpy as np

EXPECTED = {
    "fc1.weight": (128,128),
    "fc1.bias":   (128,),
    "fc2.weight": (10,128),
    "fc2.bias":   (10,),
}
KEY_ORDER = ["fc1.weight","fc1.bias","fc2.weight","fc2.bias"]

def sha256_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""): h.update(chunk)
    return h.hexdigest()

def tensor_digest(t: torch.Tensor) -> str:
    a = t.detach().cpu().contiguous().numpy()
    h = hashlib.sha256()
    h.update(str(a.dtype).encode("utf-8"))
    h.update(str(tuple(a.shape)).encode("utf-8"))
    h.update(a.tobytes(order="C"))
    return h.hexdigest()

def content_digest(state: Dict[str, torch.Tensor], key_order: List[str]) -> str:
    h = hashlib.sha256()
    for k in key_order:
        d = tensor_digest(state[k]); h.update(k.encode("utf-8")); h.update(d.encode("utf-8"))
    return h.hexdigest()

def parse_epoch_from_name(name: str) -> int:
    m = re.search(r"epoch_(\d+)", name)
    return int(m.group(1)) if m else -1

def scan_dir(ckpt_dir: str, out_csv: str) -> int:
    os.makedirs(os.path.dirname(out_csv), exist_ok=True)
    rows: List[dict] = []

    for fn in sorted(os.listdir(ckpt_dir)):
        if not (fn.endswith(".pt") or fn.endswith(".pth")):
            continue
        path = os.path.join(ckpt_dir, fn)
        size = os.path.getsize(path)
        file_sha = sha256_file(path)
        sidecar = path + ".json"

        load_ok, nan_total, inf_total = 1, 0, 0
        note_parts: List[str] = []

        # expected values from sidecar
        expected_digest = ""
        expected_file_sha = ""
        expected_digest_present = 0
        expected_file_sha_present = 0

        # load sidecar (if exists)
        if os.path.exists(sidecar):
            try:
                meta = json.loads(open(sidecar, "r", encoding="utf-8").read())
                expected_digest = str(meta.get("expected_digest", "") or "")
                expected_file_sha = str(meta.get("expected_file_sha256", "") or "")
                if expected_digest: expected_digest_present = 1
                if expected_file_sha: expected_file_sha_present = 1
            except Exception as e:
                note_parts.append(f"meta_error:{type(e).__name__}")

        # load tensors
        arrays: Dict[str, torch.Tensor] = {}
        try:
            state = torch.load(path, map_location="cpu")
            for k, v in state.items():
                if isinstance(v, torch.Tensor):
                    arrays[k] = v
                    a = v.detach().cpu().numpy()
                    nan_total += int(np.isnan(a).sum())
                    inf_total += int(np.isinf(a).sum())
        except Exception as e:
            load_ok = 0
            note_parts.append(f"load_error:{type(e).__name__}")

        # schema/shape check
        shape_ok = 0
        if load_ok:
            ok = all(k in arrays and tuple(arrays[k].shape) == EXPECTED[k] for k in EXPECTED)
            shape_ok = 1 if ok else 0
            if not ok: note_parts.append("shape_mismatch")

        # content digest check
        digest_match = 0
        if load_ok and expected_digest_present:
            try:
                d_loaded = content_digest(arrays, KEY_ORDER)
                if d_loaded == expected_digest:
                    digest_match = 1
                else:
                    note_parts.append("digest_mismatch")
            except Exception as e:
                note_parts.append(f"digest_error:{type(e).__name__}")

        # file-level hash check (container integrity)
        file_sha_match = 0
        if expected_file_sha_present:
            if file_sha == expected_file_sha:
                file_sha_match = 1
            else:
                note_parts.append("file_sha_mismatch")

        # final decision
        corrupted = int(
            (load_ok == 0) or
            (nan_total > 0) or
            (inf_total > 0) or
            (shape_ok == 0) or
            (expected_digest_present == 1 and digest_match == 0) or
            (expected_file_sha_present == 1 and file_sha_match == 0)   # NEW
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
            "expected_digest_present": expected_digest_present,
            "digest_match": digest_match,
            "expected_file_sha_present": expected_file_sha_present,  # NEW
            "file_sha_match": file_sha_match,                        # NEW
            "corrupted": corrupted,
            "note": ";".join(note_parts),
        })

    header = [
        "epoch","file","bytes","sha256","load_ok","nan_total","inf_total",
        "shape_ok","expected_digest_present","digest_match",
        "expected_file_sha_present","file_sha_match",   # NEW
        "corrupted","note"
    ]
    with open(out_csv, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=header); w.writeheader(); w.writerows(rows)

    print(f"[guard] wrote {out_csv} ({len(rows)} rows)")
    return len(rows)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--ckpt-dir", default="trace/ckpts_torch")
    ap.add_argument("--out", default="trace/guard/ckpt_scan_torch.csv")
    args = ap.parse_args()
    scan_dir(args.ckpt_dir, args.out)

if __name__ == "__main__":
    main()
