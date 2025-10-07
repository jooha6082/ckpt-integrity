#!/usr/bin/env bash
set -euo pipefail
sudo blktrace -d /dev/nvme0n1 -o - | sudo blkparse -i - > trace/blk.log
