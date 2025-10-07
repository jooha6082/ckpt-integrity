import argparse, os, random, logging
LOG = logging.getLogger("inject.flip")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("path")
    ap.add_argument("--nbytes", type=int, default=32)
    ap.add_argument("--seed", type=int, default=None)
    args = ap.parse_args()

    if args.seed is not None:
        random.seed(args.seed)
    if not os.path.exists(args.path):
        raise FileNotFoundError(args.path)

    size = os.path.getsize(args.path)
    count = min(args.nbytes, size) if size > 0 else 0
    if count == 0:
        LOG.warning("empty file: %s", args.path); return

    idxs = sorted(random.sample(range(size), count))
    with open(args.path, "rb+") as f:
        for i in idxs:
            f.seek(i)
            b = f.read(1)
            if not b:
                continue
            bit = 1 << random.randrange(8)
            f.seek(i)
            f.write(bytes([b[0] ^ bit]))
    LOG.info("bitflip: %s bytes=%d", args.path, count)

if __name__ == "__main__":
    main()
