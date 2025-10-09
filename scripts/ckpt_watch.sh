#!/usr/bin/env bash
set -euo pipefail

CKPT_DIR="${1:-ckpt}"
LAST_GOOD="${CKPT_DIR}/last-good.pt"

# wait until a file appears, is not held by any process, and size stops changing
wait_stable() {
  local f="$1"
  # wait until file exists
  while [ ! -f "$f" ]; do sleep 0.1; done
  # wait until no writer holds it
  while lsof "$f" >/dev/null 2>&1; do sleep 0.1; done
  # wait until size stabilizes between two reads
  local s1=0 s2=1
  while [ "$s1" != "$s2" ]; do s1=$(stat -f%z "$f" 2>/dev/null || stat -c%s "$f"); sleep 0.2; s2=$(stat -f%z "$f" 2>/dev/null || stat -c%s "$f"); done
}

log() { printf '[%s] %s\n' "$(date '+%Y-%m-%d %H:%M:%S %Z')" "$*" | tee -a AI_Log.txt; }

guard_verify() {
  local f="$1"
  ckpt-integrity-guard --verify "$f" --last-good "$LAST_GOOD"
}

# watch loop: on new/changed epoch_*.pt, wait stable → verify → rollback if needed
watch_dir() {
  log "watch start: ${CKPT_DIR}"
  while true; do
    for f in "${CKPT_DIR}"/epoch_*.pt; do
      [ -e "$f" ] || { sleep 0.2; continue; }
      wait_stable "$f"
      if ! out="$(guard_verify "$f" 2>&1)"; then
        log "guard error on $f: $out"
      else
        echo "$out" | tee -a AI_Log.txt
      fi
    done
    sleep 0.5
  done
}

watch_dir