#!/usr/bin/env bash
# Start the market-data-broker hub. Two modes, same defaults.
#
# Usage:
#   ./scripts/start.sh                    # local Python (default)
#   ./scripts/start.sh --python           # local Python (explicit)
#   ./scripts/start.sh --docker           # docker compose up --build
#   ./scripts/start.sh --help
#
# Env-var overrides (LOG_LEVEL, MDB_WS_PORT, MDB_STATUS_PORT, ...) work in
# either mode. See .env.example for the full list.

set -euo pipefail

cd "$(dirname "$0")/.."

mode="python"
case "${1:-}" in
  --docker) mode="docker"; shift ;;
  --python) mode="python"; shift ;;
  --help|-h)
    cat <<'EOF'
Usage: ./scripts/start.sh [--python|--docker] [extra args...]

  --python   (default) Run from the local venv via `python -m market_data_broker`.
             Bootstrap once with:
                 python3 -m venv .venv && .venv/bin/pip install -e ".[dev]"

  --docker   Run via `docker compose up --build` (foreground).
             Picks up local edits via the rebuild on each start.
             Stop with Ctrl+C — compose tears the container down cleanly.

Env vars (LOG_LEVEL, MDB_WS_PORT, MDB_STATUS_PORT, ...) propagate in either
mode. See .env.example for the full list.

Examples:
    ./scripts/start.sh
    LOG_LEVEL=DEBUG ./scripts/start.sh
    MDB_WS_PORT=9000 ./scripts/start.sh
    ./scripts/start.sh --docker
    LOG_LEVEL=DEBUG ./scripts/start.sh --docker
EOF
    exit 0
    ;;
esac

case "$mode" in
  python)
    if [[ ! -x .venv/bin/python ]]; then
      echo "no .venv found — bootstrap with:" >&2
      echo "  python3 -m venv .venv && .venv/bin/pip install -e '.[dev]'" >&2
      exit 1
    fi
    exec .venv/bin/python -m market_data_broker "$@"
    ;;
  docker)
    if ! command -v docker >/dev/null 2>&1; then
      echo "docker not found on PATH" >&2
      exit 1
    fi
    if ! docker info >/dev/null 2>&1; then
      echo "docker daemon is not running" >&2
      exit 1
    fi
    # --build picks up local edits; --remove-orphans tidies stragglers from
    # earlier compose runs. Foreground so Ctrl+C reaches compose, which
    # translates it to a clean container stop.
    exec docker compose up --build --remove-orphans "$@"
    ;;
esac
