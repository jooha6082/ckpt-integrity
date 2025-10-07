#!/usr/bin/env bash
set -euo pipefail
CKPT=${1:-ckpt/epoch_1.pt}
ckpt-integrity-inject-flip "$CKPT" --nbytes 64
ckpt-integrity-guard --verify "$CKPT" --last-good ckpt/last-good.pt
