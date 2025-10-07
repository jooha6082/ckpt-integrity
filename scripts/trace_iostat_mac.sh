#!/usr/bin/env bash
set -euo pipefail
iostat -d -k 1 > trace/iostat_mac.log
