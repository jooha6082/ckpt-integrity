#!/usr/bin/env python3
"""
Torch checkpoint writer (CPU-only) with fault/crash + digest & file-hash metadata.

- Writes state_dict (tiny MLP) as .pt bytes via torch.save().
- Sidecar JSON stores:
    * expected_digest: content hash over tensors (dtype+shape+bytes)
    * expected_file_sha256: file-level sha256 of serialized bytes (pre-fault)
- Supports atomic vs unsafe writes, crash-epoch, and pause-ms.
"""

from __future__ import annotations
import argparse, json, os, tempfile, time, hashlib, random, io, sys
from typing import Dict, List

import numpy as np
import torch
import torch.nn as nn

# ---------- IO helpers ----------
def atomic_write_bytes(path: str, data: bytes) -> None:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    fd, tmp = tempfile.mkstemp(prefix=".tmp_tckpt_", dir=os.path.dirname(path) or ".")
    try:
        with os.fdopen(fd, "wb") as f:
            f.write(data); f.flush(); os.fsync(f.fileno())
        os.replace(tmp, path)
    finally:
        try:
            if os.path.exists(tmp): os.remove(tmp)
        except FileNotFoundError:
            pass

def unsafe_write_bytes(path: str, data: bytes, partial: bool = False) -> None:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "wb") as f:
        if partial:
            cut = max(0, len(data) // 2)
            f.write(data[:cut])
        else:
            f.write(data)  # deliberately no flush/fsync

def atomic_write_text(path: str, text: str) -> None:
    atomic_write_bytes(path, text.encode("utf-8"))

# ---------- digest helpers ----------
def tensor_digest(t: torch.Tensor) -> str:
    """Hash dtype + shape + raw bytes (C-order) for determinism."""
    a = t.detach().cpu().contiguous().numpy()
    h = hashlib.sha256()
    h.update(str(a.dtype).encode("utf-8"))
    h.update(str(tuple(a.shape)).encode("utf-8"))
    h.update(a.tobytes(order="C"))
    return h.hexdigest()

def content_digest(state: Dict[str, torch.Tensor], key_order: List[str]) -> str:
    h = hashlib.sha256()
    for k in key_order:
        d = tensor_digest(state[k])
        h.update(k.encode("utf-8")); h.update(d.encode("utf-8"))
    return h.hexdigest()

def torch_bytes_from_state_dict(state: Dict[str, torch.Tensor]) -> bytes:
    """Serialize state_dict to bytes using torch.save into a BytesIO buffer."""
    buf = io.BytesIO()
    torch.save(state, buf)
    return buf.getvalue()

def sha256_bytes(b: bytes) -> str:
    h = hashlib.sha256(); h.update(b); return h.hexdigest()

# ---------- fault injection ----------
def inject_fault(raw: bytes, mode: str) -> bytes:
    if mode == "none": return raw
    ba = bytearray(raw); n = len(ba)
    if n == 0: return raw
    if mode == "bitflip":
        flips = max(1, n // 200_000)
        for _ in range(flips):
            i = random.randrange(n); bit = 1 << random.randrange(8); ba[i] ^= bit
        return bytes(ba)
    if mode == "truncate":
        keep = max(0, int(n * 0.7)); return bytes(ba[:keep])
    if mode == "zerorange":
        start = random.randrange(n); length = min(n - start, max(1, n // 100))
        for j in range(start, start + length): ba[j] = 0
        return bytes(ba)
    return raw

# ---------- model ----------
class TinyMLP(nn.Module):
    def __init__(self, d_in=128, d_h=128, d_out=10):
        super().__init__()
        self.fc1 = nn.Linear(d_in, d_h)
        self.fc2 = nn.Linear(d_h, d_out)
    def forward(self, x):
        x = torch.tanh(self.fc1(x))
        return self.fc2(x)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--epochs", type=int, default=30)
    ap.add_argument("--checkpoint-every", type=int, default=3)
    ap.add_argument("--out", type=str, default="trace/ckpts_torch")
    ap.add_argument("--seed", type=int, default=0)
    ap.add_argument("--fault", default="none", choices=["none","bitflip","truncate","zerorange"])
    ap.add_argument("--write-mode", default="atomic", choices=["atomic","unsafe"])
    ap.add_argument("--crash-epoch", type=int, default=-1)
    ap.add_argument("--pause-ms", type=int, default=0)
    args = ap.parse_args()

    os.makedirs(args.out, exist_ok=True)
    torch.manual_seed(args.seed); np.random.seed(args.seed); random.seed(args.seed)

    model = TinyMLP()
    opt = torch.optim.SGD(model.parameters(), lr=1e-2)

    key_order = ["fc1.weight","fc1.bias","fc2.weight","fc2.bias"]

    for epoch in range(1, args.epochs + 1):
        # fake one-batch "train"
        x = torch.randn(32, 128)
        y = torch.randint(0, 10, (32,))
        logits = model(x)
        loss = nn.CrossEntropyLoss()(logits, y)
        opt.zero_grad(); loss.backward(); opt.step()

        if epoch % args.checkpoint_every != 0:
            continue

        # Build state and digests (expected)
        state = {
            "fc1.weight": model.fc1.weight,
            "fc1.bias":   model.fc1.bias,
            "fc2.weight": model.fc2.weight,
            "fc2.bias":   model.fc2.bias,
        }
        expected_digest = content_digest(state, key_order)

        # Serialize to bytes (pre-fault), compute file sha (pre-fault)
        raw = torch_bytes_from_state_dict(state)
        file_sha_expected = sha256_bytes(raw)

        # Apply optional fault to serialized bytes
        raw = inject_fault(raw, args.fault)

        ckpt_path = os.path.join(args.out, f"ckpt_epoch_{epoch:04d}.pt")
        meta = {
            "epoch": epoch,
            "ts": time.time(),
            "seed": args.seed,
            "fault": args.fault,
            "write_mode": args.write_mode,
            "expected_digest": expected_digest,
            "expected_file_sha256": file_sha_expected,   # ← NEW: file-level hash
            "note": "torch-ckpt"
        }
        atomic_write_text(ckpt_path + ".json", json.dumps(meta, ensure_ascii=False))

        if args.write_mode == "atomic":
            atomic_write_bytes(ckpt_path, raw)
        else:
            partial = (args.crash_epoch > 0 and epoch == args.crash_epoch)
            unsafe_write_bytes(ckpt_path, raw, partial=partial)

        print(f"APP_EVENT,checkpoint_saved,ts={time.time():.6f},epoch={epoch},path={ckpt_path}", flush=True)
        if args.crash_epoch > 0 and epoch == args.crash_epoch:
            print(f"APP_EVENT,simulated_crash,ts={time.time():.6f},epoch={epoch}", flush=True)
            os._exit(1)

        if args.pause_ms > 0:
            time.sleep(args.pause_ms / 1000.0)

    print(f"APP_EVENT,done,epochs={args.epochs}", flush=True)

if __name__ == "__main__":
    main()
