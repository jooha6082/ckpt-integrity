#!/usr/bin/env bash
set -euo pipefail
python -m ckpt_integrity.training.train --epochs 2 --ckpt-dir ckpt --device cpu
