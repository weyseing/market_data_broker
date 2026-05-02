#!/usr/bin/env bash
# Print Claude Desktop setup + demo prompts. Echo-only.
#
#   ./scripts/claude_desktop_setup.sh           # everything
#   ./scripts/claude_desktop_setup.sh config    # config only
#   ./scripts/claude_desktop_setup.sh demo      # demo prompts only

set -euo pipefail

# ANSI colors when stdout is a tty.
if [[ -t 1 ]]; then
  H=$'\033[1;36m'   # bold cyan — section headers
  K=$'\033[1;33m'   # bold yellow — commands / keys
  D=$'\033[2m'      # dim — notes
  R=$'\033[0m'      # reset
else
  H= K= D= R=
fi

print_config() {
  cat <<EOF
${H}# Claude Desktop config${R}

File: ${K}~/Library/Application Support/Claude/claude_desktop_config.json${R}
Merge in:

  {
    "mcpServers": {
      "market-data-broker": {
        "command": "npx",
        "args": ["-y", "mcp-remote", "http://127.0.0.1:8000/mcp"]
      }
    }
  }

Then ${K}Cmd+Q${R} Claude Desktop and reopen.

${H}# Start the server first${R}

  ${K}./scripts/start.sh --service mcp${R}                # python, :8000
  ${K}./scripts/start.sh --mode docker --service mcp${R}  # docker, :8000

Wait for ${D}"Uvicorn running" + "ingest.connected"${R} (~2s).
EOF
}

print_demo() {
  cat <<EOF
${H}# Demo prompts (paste into Claude Desktop)${R}

  ${K}Discovery${R} → What can the market-data-broker MCP do?
  ${K}Snapshot${R}  → What's the current best bid/ask for BTC-USD?
  ${K}Stream${R}    → Show me 5 recent trades for ETH-USD.
  ${K}Compute${R}   → Get the current mid for BTC-USD. Show your work.
  ${K}Health${R}    → Is the hub healthy? Any reconnect loops or drops?
  ${K}E2E${R}       → Give me BTC-USD mid, last 3 trades, and feed health.
  ${K}Failure${R}   → Describe topic 'coinbase.ticker.NOPE-USD'.
  ${K}Demand${R}    → Demonstrate the demand-driven property. Call get_hub_status
              first (note ETH-USD is NOT in upstream topics), then
              stream_topic 'coinbase.matches.ETH-USD' for 5 frames,
              then get_hub_status again — explain what changed and why.

${D}Multi-client demo (no Claude needed): ./scripts/demo_multi_client.sh${R}
EOF
}

case "${1:-all}" in
  config) print_config ;;
  demo) print_demo ;;
  all)
    print_config
    echo
    print_demo
    ;;
  *)
    echo "usage: $0 [config|demo|all]" >&2
    exit 2
    ;;
esac
