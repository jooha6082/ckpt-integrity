import argparse, os, logging
LOG = logging.getLogger("inject.trunc")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("path")
    ap.add_argument("--tail", type=int, default=4096)
    args = ap.parse_args()

    if not os.path.exists(args.path):
        raise FileNotFoundError(args.path)

    size = os.path.getsize(args.path)
    new = max(0, size - args.tail)
    with open(args.path, "rb+") as f:
        f.truncate(new)
    LOG.info("truncate: %s %d -> %d", args.path, size, new)

if __name__ == "__main__":
    main()
