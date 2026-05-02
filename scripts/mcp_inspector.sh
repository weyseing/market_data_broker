#!/usr/bin/env bash
# Launch MCP Inspector. Bring up the MCP server separately first:
#
#   ./scripts/start.sh --service mcp                # python, http on :8000
#   ./scripts/start.sh --mode docker --service mcp  # docker, http on :8000
#
# Then run this script and, in the inspector UI, fill in:
#   Transport Type: Streamable HTTP
#   URL:            http://localhost:8000/mcp

set -euo pipefail

if ! command -v npx >/dev/null 2>&1; then
  echo "npx not found on PATH (install Node.js)" >&2
  exit 1
fi

exec npx @modelcontextprotocol/inspector
