#!/usr/bin/env python3
"""
Checkpoint writer (NumPy-only) with optional fault injection and crash modes.

What's new in Step 2:
- --write-mode {atomic,unsafe}: unsafe skips fsync/rename; can simulate partial writes.
- --crash-epoch N: if N matches a checkpoint epoch, simulate a hard crash immediately.
- Store an expected content digest (hash of arrays) into sidecar JSON for later verification.

The digest helps detect "silent" corruptions (e.g., bitflips) that still load.
"""

from __future__ import annotations
import argparse, hashlib, json, os, random, sys, tempfile, time
from typing import Dict, List
import numpy as np


# --------- IO helpers ----------------------------------------------------
def atomic_write_bytes(path: str, data: bytes) -> None:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    fd, tmp = tempfile.mkstemp(prefix=".tmp_ckpt_", dir=os.path.dirname(path) or ".")
    try:
        with os.fdopen(fd, "wb") as f:
            f.write(data)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path)  # atomic rename on same FS
    finally:
        try:
            if os.path.exists(tmp):
                os.remove(tmp)
        except FileNotFoundError:
            pass

def unsafe_write_bytes(path: str, data: bytes, partial: bool = False) -> None:
    """
    Non-atomic write: no fsync, no rename. If partial=True, only write the first half.
    Useful to emulate crash-in-the-middle artifacts.
    """
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "wb") as f:
        if partial:
            cut = max(0, len(data) // 2)
            f.write(data[:cut])
        else:
            f.write(data)
        # deliberately no flush/fsync

def atomic_write_text(path: str, text: str) -> None:
    atomic_write_bytes(path, text.encode("utf-8"))


# --------- bytes / digest -------------------------------------------------
def save_npz_bytes(payload: Dict[str, np.ndarray]) -> bytes:
    """Serialize arrays to canonical NPZ bytes using a temp file."""
    fd, tmp = tempfile.mkstemp(suffix=".npz")
    os.close(fd)
    try:
        np.savez(tmp, **payload)
        with open(tmp, "rb") as f:
            raw = f.read()
    finally:
        try: os.remove(tmp)
        except FileNotFoundError: pass
    return raw

def sha256_bytes(b: bytes) -> str:
    h = hashlib.sha256(); h.update(b); return h.hexdigest()

def array_digest(arr: np.ndarray) -> str:
    """
    Deterministic digest per array: dtype + shape + raw bytes (C-order).
    Avoids depending on NPZ container ordering/compression.
    """
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


# --------- fault injection -----------------------------------------------
def inject_fault(raw: bytes, mode: str) -> bytes:
    """Return possibly-corrupted bytes (mode in {none, bitflip, truncate, zerorange})."""
    if mode == "none": return raw
    ba = bytearray(raw)
    n = len(ba)
    if n == 0: return raw

    if mode == "bitflip":
        flips = max(1, n // 200_000)  # ~1 bit per ~200KB
        for _ in range(flips):
            i = random.randrange(n)
            bit = 1 << random.randrange(8)
            ba[i] ^= bit
        return bytes(ba)

    if mode == "truncate":
        keep = max(0, int(n * 0.7))
        return bytes(ba[:keep])

    if mode == "zerorange":
        start = random.randrange(n)
        length = min(n - start, max(1, n // 100))
        for j in range(start, start + length):
            ba[j] = 0
        return bytes(ba)

    return raw


# --------- main -----------------------------------------------------------
def main() -> None:
    ap = argparse.ArgumentParser(description="Checkpoint writer with faults/crash")
    ap.add_argument("--epochs", type=int, default=60)
    ap.add_argument("--checkpoint-every", type=int, default=3)
    ap.add_argument("--out", type=str, default="trace/ckpts")
    ap.add_argument("--seed", type=int, default=0)
    ap.add_argument("--fault", type=str, default="none",
                    choices=["none", "bitflip", "truncate", "zerorange"])
    ap.add_argument("--write-mode", type=str, default="atomic",
                    choices=["atomic", "unsafe"],
                    help="unsafe = no fsync/rename; may simulate partial writes")
    ap.add_argument("--crash-epoch", type=int, default=-1,
                    help="If >0, simulate crash immediately after (or during) this epoch write.")
    ap.add_argument("--pause-ms", type=int, default=0,
                help="Sleep this many milliseconds after each checkpoint save")

    args = ap.parse_args()

    os.makedirs(args.out, exist_ok=True)
    rng = np.random.default_rng(args.seed)

    # Tiny "model" state
    W1 = rng.normal(0, 0.02, size=(128, 128))
    b1 = rng.normal(0, 0.02, size=(128,))
    W2 = rng.normal(0, 0.02, size=(128, 10))
    b2 = rng.normal(0, 0.02, size=(10,))

    key_order = ["W1", "b1", "W2", "b2"]

    def step_update():
        nonlocal W1, b1, W2, b2
        W1 += rng.normal(0, 0.0005, W1.shape)
        b1 += rng.normal(0, 0.0005, b1.shape)
        W2 += rng.normal(0, 0.0005, W2.shape)
        b2 += rng.normal(0, 0.0005, b2.shape)

    for epoch in range(1, args.epochs + 1):
        step_update()
        if epoch % args.checkpoint_every != 0:
            continue

        payload = {"W1": W1, "b1": b1, "W2": W2, "b2": b2}
        # Digest BEFORE fault injection (expected content digest)
        expected_digest = content_digest(payload, key_order)

        # Serialize and possibly corrupt the file bytes
        raw = save_npz_bytes(payload)
        raw = inject_fault(raw, args.fault)

        ckpt_path = os.path.join(args.out, f"ckpt_epoch_{epoch:04d}.npz")
        meta = {
            "epoch": epoch,
            "ts": time.time(),
            "seed": args.seed,
            "fault": args.fault,
            "write_mode": args.write_mode,
            "expected_digest": expected_digest,
            "note": "ckpt-integrity-step2"
        }
        meta_path = ckpt_path + ".json"
        # Write meta first (atomically), then the checkpoint file
        atomic_write_text(meta_path, json.dumps(meta, ensure_ascii=False))

        if args.write_mode == "atomic":
            atomic_write_bytes(ckpt_path, raw)
        else:
            # unsafe: if this is the crash epoch, write only half and crash
            partial = (args.crash_epoch > 0 and epoch == args.crash_epoch)
            unsafe_write_bytes(ckpt_path, raw, partial=partial)

        # Basic event logging (stdout)
        print(f"APP_EVENT,checkpoint_saved,ts={time.time():.6f},epoch={epoch},path={ckpt_path}", flush=True)

        if args.pause_ms > 0:
            time.sleep(args.pause_ms / 1000.0)

        # Simulate a crash after write (atomic) or during write (unsafe, handled above)
        if args.crash_epoch > 0 and epoch == args.crash_epoch:
            print(f"APP_EVENT,simulated_crash,ts={time.time():.6f},epoch={epoch}", flush=True)
            os._exit(1)

    print(f"APP_EVENT,done,epochs={args.epochs}", flush=True)


if __name__ == "__main__":
    main()
