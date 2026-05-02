#!/usr/bin/env bash
# Start the market-data-broker. Two orthogonal axes: where it runs (mode)
# and what it runs (service). See ./scripts/start.sh --help for the full
# reference; the common cases are at the bottom of this header.
#
# Quick reference:
#   ./scripts/start.sh                                # python hub (default)
#   ./scripts/start.sh --service mcp                  # python mcp (stdio, default)
#   ./scripts/start.sh --service mcp --http           # python mcp (http)
#   ./scripts/start.sh --mode docker                  # docker hub
#   ./scripts/start.sh --mode docker --service mcp    # docker mcp on :8000
#   ./scripts/start.sh --mode docker --service all    # docker hub + mcp

set -euo pipefail

cd "$(dirname "$0")/.."

print_help() {
  cat <<'EOF'
Usage: ./scripts/start.sh [--mode python|docker] [--service hub|mcp|all] [args...]

  --mode      python (default) | docker
  --service   hub (default) | mcp | all      (all = docker only)
  --help      Show this help.

Python+mcp defaults to --stdio (Claude Desktop / inspector). Pass --http
for HTTP transport (curl / remote agents). Docker+mcp is always --http.

Examples:
  ./scripts/start.sh                              # python hub
  ./scripts/start.sh --service mcp                # python mcp (stdio, Claude Desktop / inspector)
  ./scripts/start.sh --service mcp --http         # python mcp (http on :8000)
  ./scripts/start.sh --mode docker                # docker hub
  ./scripts/start.sh --mode docker --service mcp  # docker mcp on :8000
  ./scripts/start.sh --mode docker --service all  # docker hub + mcp

Python mode forwards extra args to `python -m market_data_broker`.
Docker mode rejects extra args — use .env or docker-compose.yml.
EOF
}

mode="python"
service="hub"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --mode)
      [[ $# -ge 2 ]] || { echo "error: --mode requires a value (python|docker)" >&2; exit 1; }
      case "$2" in
        python|docker) mode="$2" ;;
        *) echo "error: --mode must be 'python' or 'docker', got '$2'" >&2; exit 1 ;;
      esac
      shift 2
      ;;
    --service)
      [[ $# -ge 2 ]] || { echo "error: --service requires a value (hub|mcp|all)" >&2; exit 1; }
      case "$2" in
        hub|mcp|all) service="$2" ;;
        *) echo "error: --service must be 'hub', 'mcp', or 'all', got '$2'" >&2; exit 1 ;;
      esac
      shift 2
      ;;
    --help|-h)
      print_help
      exit 0
      ;;
    *)
      # First unrecognised token — leave it (and the rest) in "$@" for
      # mode-specific passthrough handling below.
      break
      ;;
  esac
done

if [[ "$mode" == "python" && "$service" == "all" ]]; then
  echo "error: --service all is docker-only (a single Python process runs hub OR mcp, not both)" >&2
  exit 1
fi

case "$mode" in
  python)
    if [[ ! -x .venv/bin/python ]]; then
      echo "no .venv found — bootstrap with:" >&2
      echo "  python3 -m venv .venv && .venv/bin/pip install -e '.[dev]'" >&2
      exit 1
    fi
    case "$service" in
      hub)
        # Hub is the default subcommand of `python -m market_data_broker`.
        exec .venv/bin/python -m market_data_broker "$@"
        ;;
      mcp)
        # Transport default lives in argparse (mcp.set_defaults(transport="stdio")
        # in __main__.py) — pass --http explicitly for HTTP. Extra args
        # (--http / --host / --port) flow through unchanged.
        exec .venv/bin/python -m market_data_broker mcp "$@"
        ;;
    esac
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
    if [[ $# -gt 0 ]]; then
      echo "error: docker mode does not accept extra args: $*" >&2
      echo "(edit .env at the repo root or docker-compose.yml for overrides)" >&2
      exit 1
    fi
    # --build picks up local edits; --remove-orphans tidies stragglers from
    # earlier compose runs. Foreground so Ctrl+C reaches compose, which
    # translates it to a clean container stop.
    case "$service" in
      hub) exec docker compose up --build --remove-orphans hub ;;
      mcp) exec docker compose up --build --remove-orphans mcp ;;
      all) exec docker compose up --build --remove-orphans ;;
    esac
    ;;
esac
