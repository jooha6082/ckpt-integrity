#!/usr/bin/env bash
set -euo pipefail
iostat -xm 1 > trace/iostat_linux.log
