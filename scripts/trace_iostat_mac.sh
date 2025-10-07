#!/usr/bin/env bash
# quick iostat tracer (mac/linux)

set -e

here="$(cd "$(dirname "$0")" && pwd)"
root="$here/.."
logdir="$root/trace"
mkdir -p "$logdir"


iostat_bin="$(command -v iostat || true)"
[[ -z "$iostat_bin" && -x /usr/sbin/iostat ]] && iostat_bin=/usr/sbin/iostat
[[ -x "$iostat_bin" ]] || { echo "iostat not found"; exit 1; }

case "$(uname)" in
  Darwin)
    logfile="$logdir/iostat_mac.log"
    "$iostat_bin" -d -w 1 >> "$logfile" 2>&1 &
    ;;
  *)
    logfile="$logdir/iostat_linux.log"
    "$iostat_bin" -d -k 1 >> "$logfile" 2>&1 &
    ;;
esac

echo $! > "$logdir/iostat.pid"
echo "iostat pid $(cat "$logdir/iostat.pid") → $logfile"
echo "stop: kill \$(cat \"$logdir/iostat.pid\")"