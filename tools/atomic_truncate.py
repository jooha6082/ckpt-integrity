#!/usr/bin/env python3
"""
Atomically truncate a file to the first N bytes.
Useful when CLI injector for truncate is not available.
"""
import argparse, os, tempfile

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("path", help="file to truncate")
    ap.add_argument("--keep-bytes", type=int, required=True, help="bytes to keep from the start")
    args = ap.parse_args()

    d = os.path.dirname(args.path) or "."
    with open(args.path, "rb") as f:
        data = f.read(args.keep_bytes)

    fd, tmp = tempfile.mkstemp(prefix=".trunc.", dir=d)
    os.close(fd)
    with open(tmp, "wb") as w:
        w.write(data)
    os.replace(tmp, args.path)
    print(f"truncated: {args.path} -> {args.keep_bytes} bytes")

if __name__ == "__main__":
    main()
