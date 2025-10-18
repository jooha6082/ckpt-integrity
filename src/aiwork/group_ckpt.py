#!/usr/bin/env python3
"""
Group checkpoint writer with manifest+commit and directory fsync toggle.

- Per epoch writes three parts: model.bin, optim.bin, rng.json
- Atomic mode:
    * each part: write tmp -> fsync(file) -> atomic rename
    * MANIFEST.json.tmp -> fsync -> rename
    * COMMIT.json -> fsync(file) and (optional) fsync(parent dir)
- Unsafe mode:
    * direct writes; optional partial writes / early exits to simulate crashes
- Options:
    * --dir-fsync / --no-dir-fsync : toggle fsync(parent directory) after COMMIT
    * --pause-ms : pacing between checkpoints (observability parity)
    * --kb-model / --kb-optim : payload size knobs for scaling experiments
"""
from __future__ import annotations
import argparse, os, json, tempfile, time, hashlib, random
from argparse import BooleanOptionalAction
from pathlib import Path
from typing import Dict

# ---------- small IO helpers ----------
def sha256_bytes(b: bytes) -> str:
    h = hashlib.sha256(); h.update(b); return h.hexdigest()

def fsync_dir(path: Path) -> None:
    dfd = os.open(str(path), os.O_RDONLY)
    try:
        os.fsync(dfd)
    finally:
        os.close(dfd)

def atomic_write_bytes(path: Path, data: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(prefix=".tmp_gc_", dir=str(path.parent))
    try:
        with os.fdopen(fd, "wb") as f:
            f.write(data); f.flush(); os.fsync(f.fileno())
        os.replace(tmp, path)  # atomic rename
    finally:
        try:
            if os.path.exists(tmp): os.remove(tmp)
        except FileNotFoundError:
            pass

def unsafe_write_bytes(path: Path, data: bytes, partial: bool=False) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "wb") as f:
        if partial and len(data) > 1:
            cut = max(1, len(data)//2)
            f.write(data[:cut])
        else:
            f.write(data)

# ---------- payload generation ----------
def gen_parts(seed: int, epoch: int, kb_model: int, kb_optim: int) -> Dict[str, bytes]:
    random.seed(seed*1000 + epoch)
    model = os.urandom(kb_model * 1024)
    optim = os.urandom(kb_optim * 1024)
    rng   = json.dumps({"seed": seed, "epoch": epoch, "ts": time.time()}, sort_keys=True).encode("utf-8")
    return {"model.bin": model, "optim.bin": optim, "rng.json": rng}

def inject_fault(b: bytes, mode: str) -> bytes:
    if mode == "none": return b
    ba = bytearray(b); n = len(ba)
    if n == 0: return b
    if mode == "bitflip":
        i = random.randrange(n); ba[i] ^= 1 << random.randrange(8); return bytes(ba)
    if mode == "truncate":
        keep = max(1, n//2); return bytes(ba[:keep])
    if mode == "zerorange":
        start = random.randrange(n); length = min(n-start, max(1, n//100))
        for j in range(start, start+length): ba[j] = 0
        return bytes(ba)
    return b

# ---------- writer core ----------
def write_group(out_root: Path, epoch: int, seed: int, write_mode: str,
                fault: str, crash_at: str, kb_model: int, kb_optim: int,
                dir_fsync: bool) -> None:
    ep_dir = out_root / f"epoch_{epoch:04d}"
    ep_dir.mkdir(parents=True, exist_ok=True)

    parts = gen_parts(seed, epoch, kb_model, kb_optim)
    manifest = []

    # 1) write parts
    for name, data in parts.items():
        data = inject_fault(data, fault)
        p = ep_dir / name
        if write_mode == "atomic":
            atomic_write_bytes(p, data)  # tmp+fsync+rename inside
        else:
            partial = (name == "model.bin" and crash_at == "after_model")
            unsafe_write_bytes(p, data, partial=partial)
        manifest.append({"path": name, "bytes": len(data), "sha256": sha256_bytes(data)})

    # crash point before manifest
    if write_mode == "unsafe" and crash_at == "before_manifest":
        os._exit(2)

    # 2) write MANIFEST
    man = {"epoch": epoch, "seed": seed, "parts": manifest}
    man_bytes = json.dumps(man, sort_keys=True).encode("utf-8")
    man_path = ep_dir / "MANIFEST.json"
    if write_mode == "atomic":
        atomic_write_bytes(man_path, man_bytes)
    else:
        unsafe_write_bytes(man_path, man_bytes, partial=(crash_at == "manifest_partial"))

    if write_mode == "unsafe" and crash_at in ("before_commit","manifest_partial"):
        os._exit(3)

    # 3) write COMMIT + optional fsync(dir)
    commit = {"epoch": epoch, "seed": seed, "manifest_sha256": sha256_bytes(man_bytes), "ts": time.time()}
    com_path = ep_dir / "COMMIT.json"
    if write_mode == "atomic":
        atomic_write_bytes(com_path, json.dumps(commit, sort_keys=True).encode("utf-8"))
        if dir_fsync:
            fsync_dir(ep_dir)
    else:
        unsafe_write_bytes(com_path, json.dumps(commit, sort_keys=True).encode("utf-8"))

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="trace/groups/demo")
    ap.add_argument("--epochs", type=int, default=12)
    ap.add_argument("--every", type=int, default=3)
    ap.add_argument("--seed", type=int, default=0)
    ap.add_argument("--write-mode", choices=["atomic","unsafe"], default="atomic")
    ap.add_argument("--fault", choices=["none","bitflip","truncate","zerorange"], default="none")
    ap.add_argument("--crash-at", choices=["none","after_model","before_manifest","manifest_partial","before_commit"], default="none")
    ap.add_argument("--pause-ms", type=int, default=0)
    ap.add_argument("--kb-model", type=int, default=128)
    ap.add_argument("--kb-optim", type=int, default=64)
    # Boolean toggle with --dir-fsync / --no-dir-fsync (py>=3.9)
    ap.add_argument("--dir-fsync", action=BooleanOptionalAction, default=True,
                    help="fsync the parent directory after COMMIT (default: on)")
    args = ap.parse_args()

    for e in range(args.every, args.epochs + 1, args.every):
        write_group(Path(args.out), e, args.seed, args.write_mode, args.fault, args.crash_at,
                    args.kb_model, args.kb_optim, args.dir_fsync)
        if args.pause_ms > 0:
            time.sleep(args.pause_ms / 1000.0)

if __name__ == "__main__":
    main()
