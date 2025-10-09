#!/usr/bin/env bash
set -euo pipefail

# -------- Configuration --------
LOG_DIR="trace"
LOG_FILE="$LOG_DIR/iostat_mac.log"
PID_FILE="$LOG_DIR/iostat_mac.pid"
# Sampling interval in seconds (macOS iostat uses -w)
INTERVAL="${INTERVAL:-1}"
# --------------------------------

rotate_log() {
  # Rotate existing log so each run has a clean file
  if [[ -f "$LOG_FILE" && -s "$LOG_FILE" ]]; then
    local ts
    ts="$(date +%Y%m%d-%H%M%S)"
    mv "$LOG_FILE" "${LOG_FILE%.*}.$ts.log"
  fi
}

start() {
  mkdir -p "$LOG_DIR"

  # If a PID file exists and the process is alive, do nothing
  if [[ -f "$PID_FILE" ]]; then
    local pid
    pid="$(cat "$PID_FILE")" || true
    if [[ -n "${pid:-}" ]] && kill -0 "$pid" 2>/dev/null; then
      echo "iostat already running (pid $pid) → $LOG_FILE"
      exit 0
    fi
    # Stale PID file: remove it
    rm -f "$PID_FILE"
  fi

  rotate_log
  {
    echo "# iostat (macOS) started: $(date)"
    echo "# cmd: iostat -d -w $INTERVAL"
  } >> "$LOG_FILE"

  # macOS iostat: -d (disks), -w <interval>. No '-k' on macOS.
  nohup iostat -d -w "$INTERVAL" >> "$LOG_FILE" 2>&1 &
  echo $! > "$PID_FILE"
  echo "started iostat (pid $(cat "$PID_FILE")) → $LOG_FILE"
}

stop() {
  if [[ -f "$PID_FILE" ]]; then
    local pid
    pid="$(cat "$PID_FILE")" || true
    if [[ -n "${pid:-}" ]] && kill -0 "$pid" 2>/dev/null; then
      kill "$pid" 2>/dev/null || true
      # Give it a moment to exit
      sleep 0.5
      # Force kill if still alive
      kill -9 "$pid" 2>/dev/null || true
    fi
    rm -f "$PID_FILE"
    echo "stopped iostat; log at $LOG_FILE"
  else
    echo "nothing to stop (no PID file)"
  fi
}

status() {
  if [[ -f "$PID_FILE" ]]; then
    local pid
    pid="$(cat "$PID_FILE")" || true
    if [[ -n "${pid:-}" ]] && kill -0 "$pid" 2>/dev/null; then
      echo "running (pid $pid) → $LOG_FILE"
      # Show last few lines for quick sanity check
      tail -n 3 "$LOG_FILE" || true
      exit 0
    fi
  fi
  echo "not running"
  exit 1
}

usage() {
  echo "usage: $0 {start|stop|status}"
}

cmd="${1:-start}"
case "$cmd" in
  start)  start ;;
  stop)   stop ;;
  status) status ;;
  *)      usage; exit 1 ;;
esac
