#!/usr/bin/env python3
"""
Group integrity scanner:
- For each epoch dir under --root:
  * require COMMIT.json
  * verify MANIFEST.json exists, matches COMMIT.manifest_sha256
  * verify each part path exists and matches bytes+sha256
- Outputs CSV: epoch,dir,has_commit,has_manifest,parts_ok,group_ok,note
"""
from __future__ import annotations
import argparse, json, os, csv, hashlib
from pathlib import Path

def sha256_file(p: Path) -> str:
    h = hashlib.sha256()
    with open(p, "rb") as f:
        for chunk in iter(lambda: f.read(1<<20), b""): h.update(chunk)
    return h.hexdigest()

def scan_dir(root: str, out_csv: str) -> int:
    rows=[]
    for ep_dir in sorted(Path(root).glob("epoch_*")):
        epoch = int(str(ep_dir.name).split("_")[-1])
        com = ep_dir / "COMMIT.json"
        man = ep_dir / "MANIFEST.json"
        note=[]
        has_commit = int(com.exists())
        has_manifest = int(man.exists())
        parts_ok = 0
        group_ok = 0

        manifest_sha = ""
        manifest = None
        if has_manifest:
            try:
                b = man.read_bytes()
                manifest_sha = hashlib.sha256(b).hexdigest()
                manifest = json.loads(b.decode("utf-8"))
            except Exception as e:
                note.append(f"manifest_error:{type(e).__name__}")

        if not has_commit:
            note.append("no_commit")
        else:
            try:
                c = json.loads(com.read_text(encoding="utf-8"))
                if manifest is None:
                    pass
                elif c.get("manifest_sha256","") != manifest_sha:
                    note.append("commit_manifest_mismatch")
                else:
                    failures = 0
                    for pt in manifest.get("parts", []):
                        p = ep_dir / pt["path"]
                        if not p.exists():
                            failures += 1; note.append(f"missing:{pt['path']}"); continue
                        if os.path.getsize(p) != int(pt["bytes"]):
                            failures += 1; note.append(f"size_mismatch:{pt['path']}")
                        elif sha256_file(p) != pt["sha256"]:
                            failures += 1; note.append(f"sha_mismatch:{pt['path']}")
                    parts_ok = int(failures == 0)
                    group_ok = int(parts_ok == 1)
            except Exception as e:
                note.append(f"commit_error:{type(e).__name__}")

        rows.append({
            "epoch": epoch,
            "dir": str(ep_dir),
            "has_commit": has_commit,
            "has_manifest": has_manifest,
            "parts_ok": parts_ok,
            "group_ok": group_ok,
            "note": ";".join(note)
        })

    Path(out_csv).parent.mkdir(parents=True, exist_ok=True)
    with open(out_csv, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["epoch","dir","has_commit","has_manifest","parts_ok","group_ok","note"])
        w.writeheader(); w.writerows(rows)
    print(f"[group_guard] wrote {out_csv} ({len(rows)})")
    return len(rows)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", default="trace/groups/demo")
    ap.add_argument("--out", default="trace/guard/group_scan.csv")
    args = ap.parse_args()
    scan_dir(args.root, args.out)

if __name__ == "__main__":
    main()
