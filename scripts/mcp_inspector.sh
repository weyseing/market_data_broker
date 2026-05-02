#!/usr/bin/env bash
# Launch MCP Inspector pre-wired to the HTTP MCP server. Click "Connect"
# in the inspector UI — the transport and URL are already filled in.
#
# Bring the server up first (HTTP is the python default since __main__.py):
#
#   ./scripts/start.sh --service mcp                # python, http on :8000
#   ./scripts/start.sh --mode docker --service mcp  # docker, http on :8000
#
# For a stdio inspector test (no separate server process), bypass this script:
#
#   npx @modelcontextprotocol/inspector .venv/bin/python \
#     -m market_data_broker mcp --stdio

set -euo pipefail

cd "$(dirname "$0")/.."

URL="http://localhost:8000/mcp"

if ! command -v npx >/dev/null 2>&1; then
  echo "npx not found on PATH (install Node.js)" >&2
  exit 1
fi

# Friendly nudge if nothing is listening — the inspector won't tell you
# clearly, it just fails on Connect.
if ! lsof -iTCP:8000 -sTCP:LISTEN -P >/dev/null 2>&1; then
  echo "warning: nothing listening on :8000 — start the server first:" >&2
  echo "  ./scripts/start.sh --service mcp" >&2
  echo >&2
fi

exec npx @modelcontextprotocol/inspector \
  --transport http --server-url "$URL"
