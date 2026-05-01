#!/usr/bin/env bash
# Start the market-data-broker hub from the local venv.
#
# Usage:
#   ./scripts/start.sh
#   MDB_DEBUG_SUBSCRIBE=coinbase.ticker.BTC-USD ./scripts/start.sh
#   LOG_LEVEL=DEBUG ./scripts/start.sh
set -euo pipefail

cd "$(dirname "$0")/.."

if [[ ! -x .venv/bin/python ]]; then
  echo "no .venv found — bootstrap with:" >&2
  echo "  python3 -m venv .venv && .venv/bin/pip install -e '.[dev]'" >&2
  exit 1
fi

exec .venv/bin/python -m market_data_broker "$@"
