#!/usr/bin/env bash
# Usage: source scripts/venv_on.sh
# Purpose: Activate project venv in a consistent way.

# Resolve repo root (works when sourced)
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

if [[ -d "$ROOT/.venv" ]]; then
  # Default venv
  # shellcheck disable=SC1091
  source "$ROOT/.venv/bin/activate"
elif [[ -d "$ROOT/.venv-1" ]]; then
  # Fallback venv
  # shellcheck disable=SC1091
  source "$ROOT/.venv-1/bin/activate"
else
  echo "[ERR] No .venv or .venv-1 found under $ROOT" >&2
  return 1 2>/dev/null || exit 1
fi

echo "[venv] $(python -V 2>&1) @ $VIRTUAL_ENV"