#!/usr/bin/env bash
# Multi-client demand-driven demo. Visualises:
#   (a) fan-out — multiple consumers share one upstream Coinbase sub
#   (b) ref-count teardown — upstream unsubscribes ONLY when the LAST holder leaves
#
# This is the "no leaked upstream subs" guarantee — pinned by
# tests/test_registry.py::test_full_lifecycle_no_leaks.
#
# Pre-req: hub running.
#   ./scripts/start.sh                # python hub
#   ./scripts/start.sh --mode docker  # docker hub

set -euo pipefail
cd "$(dirname "$0")/.."

# ANSI colors only on a tty.
if [[ -t 1 ]]; then
  H=$'\033[1;36m'   # cyan headers
  G=$'\033[1;32m'   # green spawn
  Y=$'\033[1;33m'   # yellow stop
  D=$'\033[2m'      # dim notes
  R=$'\033[0m'
else
  H= G= Y= D= R=
fi

STATUS_URL="${STATUS_URL:-http://localhost:8080/status}"
PIDS=()

cleanup() {
  echo
  echo "${Y}cleaning up clients...${R}"
  for pid in "${PIDS[@]:-}"; do
    [[ -n "$pid" ]] && kill "$pid" 2>/dev/null || true
  done
  wait 2>/dev/null || true
}
trap cleanup EXIT INT TERM

if ! curl -sf "$STATUS_URL" >/dev/null 2>&1; then
  echo "error: hub not reachable at $STATUS_URL" >&2
  echo "start it first:  ./scripts/start.sh" >&2
  exit 1
fi
if [[ ! -x .venv/bin/python ]]; then
  echo "error: no .venv (need it for the smoke client + JSON parsing)" >&2
  exit 1
fi

show_state() {
  local label="$1"
  echo
  echo "${H}── ${label}${R}"
  curl -s "$STATUS_URL" | .venv/bin/python -c '
import json, sys
s = json.load(sys.stdin)
consumers = s.get("consumers", [])
topics = s.get("topics", [])
upstream_topics = sorted({t for u in s.get("upstreams", []) for t in u.get("topics", [])})

print(f"  consumers ({len(consumers)}):")
if consumers:
    for c in consumers:
        cid = c.get("consumer_id")
        held = sorted(c.get("topics", []))
        print(f"    {cid}: {held}")
else:
    print("    (none)")

print("  topic refcounts:")
if topics:
    for t in topics:
        name = t["topic"]
        cnt = t["subscriber_count"]
        print(f"    {name:40s} holders={cnt}")
else:
    print("    (none)")

if upstream_topics:
    print(f"  upstream subscribed: {upstream_topics}")
else:
    print("  upstream subscribed: (none)")
'
}

spawn() {
  local id="$1" topic="$2"
  echo "${G}+ spawn ${id} → ${topic}${R}"
  # 10000 frames is well beyond demo runtime; the client lives until killed.
  .venv/bin/python scripts/smoke_ws_client.py "$topic" 10000 \
    >/tmp/mdb_demo_${id}.log 2>&1 &
  PIDS+=($!)
}

stop_at() {
  local idx="$1" id="$2"
  local pid="${PIDS[$idx]}"
  echo "${Y}- stop ${id} (pid ${pid})${R}"
  kill "$pid" 2>/dev/null || true
  wait "$pid" 2>/dev/null || true
}

# --- demo timeline ---

show_state "T0  before any client"

spawn A coinbase.ticker.BTC-USD
sleep 2
show_state "T1  client A subscribed   (0→1 BTC ticker: ingest sent SUBSCRIBE upstream)"

spawn B coinbase.ticker.BTC-USD
sleep 2
show_state "T2  client B same topic   (1→2 BTC ticker: fan-out, no new upstream sub)"

spawn C coinbase.matches.ETH-USD
sleep 2
show_state "T3  client C new topic    (0→1 ETH matches: ingest sent SUBSCRIBE upstream)"

stop_at 0 A
sleep 1
show_state "T4  stop A                (2→1 BTC ticker: B still holds, upstream stays)"

stop_at 1 B
sleep 1
show_state "T5  stop B                (1→0 BTC ticker: ingest sent UNSUBSCRIBE upstream)"

stop_at 2 C
sleep 1
show_state "T6  stop C                (1→0 ETH matches: ingest sent UNSUBSCRIBE; clean)"

echo
echo "${G}done${R} — 'upstream subscribed' above should be (none)"
echo "${D}per-client logs: /tmp/mdb_demo_{A,B,C}.log${R}"
