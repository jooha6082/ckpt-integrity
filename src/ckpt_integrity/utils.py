import hashlib, json, os, pathlib, time

def sha256_file(path: str, chunk: int = 1 << 20) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for b in iter(lambda: f.read(chunk), b""):
            h.update(b)
    return h.hexdigest()

def write_meta(ckpt_path: str, step: int, extra: dict | None = None) -> tuple[str, dict]:
    meta = {
        "ckpt": pathlib.Path(ckpt_path).name,
        "step": int(step),
        "sha256": sha256_file(ckpt_path),
        "mtime": os.path.getmtime(ckpt_path),
        "ts": time.time(),
    }
    if extra:
        meta.update(extra)
    meta_path = os.path.splitext(ckpt_path)[0] + ".meta.json"
    with open(meta_path, "w") as f:
        json.dump(meta, f, indent=2, ensure_ascii=False)
    return meta_path, meta

def read_meta(ckpt_path: str) -> dict | None:
    meta_path = os.path.splitext(ckpt_path)[0] + ".meta.json"
    if not os.path.exists(meta_path):
        return None
    with open(meta_path) as f:
        return json.load(f)
