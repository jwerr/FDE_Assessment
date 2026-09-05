#!/usr/bin/env bash
# Master script: sets up and verifies all four tasks from the repo root.
#
#   ./run_all.sh            install deps + run every test suite
#   ./run_all.sh --demo     …and run each task's demo.sh afterwards
#   ./run_all.sh --clean    remove all .venv/, caches, logs, sqlite files
#
# One virtualenv per task (they pin different dependencies).
# Works on macOS default bash 3.2 and on Linux.
set -u
cd "$(dirname "$0")"

TASKS="mcp-customer-server mcp-gateway llm-guardrail llm-router"
PYTHON="${PYTHON:-python3}"
RUN_DEMO=0
SUMMARY=""

case "${1:-}" in
  --demo)  RUN_DEMO=1 ;;
  --clean)
    for t in $TASKS; do
      rm -rf "$t/.venv" "$t/__pycache__" "$t/.pytest_cache" "$t"/*.log "$t"/*.sqlite3 "$t"/*.sqlite3-wal "$t"/*.sqlite3-shm
    done
    echo "cleaned."; exit 0 ;;
  "") ;;
  *) echo "usage: $0 [--demo|--clean]"; exit 2 ;;
esac

if ! "$PYTHON" -c 'import sys; sys.exit(0 if sys.version_info >= (3,10) else 1)'; then
  echo "!! Python 3.10+ required (found: $("$PYTHON" --version 2>&1))"; exit 1
fi

banner () { echo; echo "════════════════════════════════════════════════════════════"; echo "  $1"; echo "════════════════════════════════════════════════════════════"; }

for t in $TASKS; do
  banner "$t"
  if [ ! -d "$t" ]; then echo "!! folder $t missing"; SUMMARY="$SUMMARY
  $t  MISSING"; continue; fi

  # --- venv + deps ---------------------------------------------------------
  if [ ! -x "$t/.venv/bin/python" ]; then
    echo "→ creating venv"
    "$PYTHON" -m venv "$t/.venv" || { SUMMARY="$SUMMARY
  $t  VENV FAILED"; continue; }
  fi
  echo "→ installing requirements"
  "$t/.venv/bin/pip" install -q -r "$t/requirements.txt" || { SUMMARY="$SUMMARY
  $t  PIP FAILED"; continue; }

  # --- tests ----------------------------------------------------------------
  echo "→ running tests"
  ( cd "$t" && .venv/bin/python -m pytest -q -p no:aiohttp 2>&1 | grep -vi warning | tail -3 )
  ( cd "$t" && .venv/bin/python -m pytest -q -p no:aiohttp >/dev/null 2>&1 )
  rc=$?
  if [ $rc -eq 0 ]; then status="PASS"; else status="FAIL"; fi

  # --- demo (optional) ------------------------------------------------------
  if [ $RUN_DEMO -eq 1 ] && [ -x "$t/demo.sh" ]; then
    echo; echo "→ demo"
    ( cd "$t" && PATH="$PWD/.venv/bin:$PATH" ./demo.sh ) || status="$status (demo error)"
  fi

  SUMMARY="$SUMMARY
  $t  $status"
done

banner "SUMMARY"
echo "$SUMMARY"
echo
case "$SUMMARY" in
  *FAIL*|*MISSING*) exit 1 ;;
  *) echo "all suites passed."; exit 0 ;;
esac
