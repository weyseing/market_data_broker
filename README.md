# Market Data Broker

A real-time crypto market-data hub. Ingests from Coinbase, distributes to many
downstream consumers via a single demand-driven pub/sub bus, exposes itself
both over plain WebSocket (for general clients) and over MCP (for LLM agents,
Step 8 — pending).

Status: Steps 1–7 + Step 10 done. MCP server pending. See
[progress/20260425_implementation_plan.txt](progress/20260425_implementation_plan.txt).

---

## Quickstart

Requires Python 3.11+.

```bash
python3 -m venv .venv
.venv/bin/pip install -e ".[dev]"
./scripts/start.sh
```

You'll see JSON logs on stdout — `hub.starting`, `ws_server.started`,
`status_server.started`, `ingest.connected`, `hub.ready`. Ctrl+C to stop;
shutdown takes ~1–3s while ingest closes the upstream WS cleanly.

The hub now exposes:

- **WebSocket** at `ws://localhost:8765` — for streaming consumers
- **HTTP** at `http://localhost:8080/status` — for operators / health checks

Nothing is upstream-subscribed yet — the hub is **demand-driven**. It only
asks Coinbase for a topic once a consumer has actually subscribed.

### Smoke-test it

In another terminal:

```bash
# Stream 5 BTC-USD ticker frames from real Coinbase
.venv/bin/python scripts/smoke_ws_client.py coinbase.ticker.BTC-USD 5

# Inspect hub state
curl -s http://localhost:8080/status | python -m json.tool

# Liveness ping (for container health checks)
curl -s http://localhost:8080/healthz
```

The `/status` response after a subscribe shows the consumer, the active topic,
and the upstream Coinbase connection state — all updated in real time.

---

## What runs

```
                     Coinbase WSS
                         │
                         ▼
                  ┌─────────────┐
                  │   ingest    │  reconnect, watchdog, demand reconcile
                  └──────┬──────┘
                         │ Message
                         ▼
                  ┌─────────────┐
                  │     bus     │  in-memory pub/sub, drop-oldest backpressure
                  └──────┬──────┘
                         │
              ┌──────────┼──────────┐
              ▼          ▼          ▼
        ┌─────────┐ ┌────────┐ ┌──────────┐
        │ snapshot│ │registry│ │ ws_server│  ← downstream WS clients
        └─────────┘ └────────┘ └──────────┘
                         │
                         ▼
                  ┌────────────────┐
                  │ status_server  │  ← GET /status, GET /healthz
                  └────────────────┘
```

- **ingest** owns one WebSocket to Coinbase. Reconnects with exponential
  backoff. Stale-feed watchdog forces a reconnect if no frame arrives within
  `heartbeat_timeout_seconds`.
- **registry** ref-counts subscriptions. When a topic transitions 0→1 holders
  it tells ingest to send an upstream `subscribe`; on 1→0 it sends an
  `unsubscribe`. **No leaked upstream subs** — verified by
  `test_disconnect_releases_topics_no_leak`.
- **bus** is a routing primitive: each subscriber owns a bounded queue,
  drop-oldest on overflow, dropped-message counter per consumer.
- **snapshot** is a passive bus observer — caches last ticker / trade /
  best-bid-ask per symbol. *Not* a registry consumer (would create artificial
  upstream demand).
- **server.ws** = downstream WebSocket fan-out. Each connection is a registry
  consumer.
- **server.status** = HTTP `/status` + `/healthz`.

The MCP server (Step 8) will sit beside `ws.py` and `status.py` as a third
sibling under `server/`, sharing the same registry and snapshot.

---

## Run modes

### Plain (development)

```bash
./scripts/start.sh
```

### With env-var overrides

Defaults come from [config/default.yaml](config/default.yaml). Any value can
be overridden by an env var — see [.env.example](.env.example) for the full
list.

```bash
# Bind to a different WS port
MDB_WS_PORT=9000 ./scripts/start.sh

# Verbose logs
LOG_LEVEL=DEBUG ./scripts/start.sh

# Pre-subscribe a debug consumer (useful for manual ingest verification)
MDB_DEBUG_SUBSCRIBE=coinbase.ticker.BTC-USD ./scripts/start.sh

# Source a .env file persistently
set -a; source .env; set +a; ./scripts/start.sh
```

### Docker (Step 11 — pending)

A Dockerfile + docker-compose.yml will land in Step 11.

---

## Configuration

Three layers, highest wins:

1. **Environment variables** — see [.env.example](.env.example).
2. **YAML** — [config/default.yaml](config/default.yaml).
3. **Pydantic model defaults** in [config.py](src/market_data_broker/config.py).

The config object is built once at startup
([__main__.py](src/market_data_broker/__main__.py)) and flows into each
component via constructor kwargs. **No module other than `__main__.py` reads
env vars.** Tests construct components directly with kwargs and don't need to
manipulate the environment.

---

## Topic naming

```
{venue}.{channel}.{product_id}     e.g. coinbase.ticker.BTC-USD
```

Channels in scope today:

| Channel | What it carries | Cadence |
|---|---|---|
| `ticker` | One ticker tick (last trade + post-trade top-of-book) | One per trade (~5 Hz on BTC) |
| `matches` | One executed trade | One per trade |
| `level2_batch` | L2 book snapshot then incremental updates | Snapshot once + diffs continuously |

The `venue.` prefix leaves room for a future `binance.*` ingest without
schema or wire changes.

---

## Backpressure

- Each consumer's bus subscription has a bounded queue
  (`bus.consumer_queue_size`, default 1000).
- On overflow, the bus **drops the oldest** message and increments
  `dropped_messages` for that consumer.
- Per-connection cumulative drop cap (`bus.sustained_overflow_drops`, default
  100) — exceeding it gets the consumer disconnected with WS close code 1013.

Drop-oldest is intentional: a slow client cannot stall publishers or peers,
but its own backlog gets trimmed.

---

## Restart-state caveats

The hub holds **all state in memory**. On restart you lose:

| State | Recovery |
|---|---|
| Snapshot store cache (last ticker, trade, top-of-book) | Repopulated as soon as the first messages flow on each tracked topic |
| Active consumer subscriptions | Each consumer must reconnect and re-subscribe — clients are responsible for retry/backoff |
| Upstream subscriptions to Coinbase | Reconciled from new downstream demand on the next session |
| Registry message counters / msg-rate stats | Reset to zero |

There is **no database**, no persistence layer. This is by design — the hub
is a streaming pipe, not a store of record.

---

## Layout

```
src/market_data_broker/
├── __main__.py        — wires everything; entry point for python -m
├── config.py          — Pydantic Config model + YAML/env loader
├── bus.py             — InMemoryBus + Subscription
├── registry.py        — ref-counts consumers, drives demand callbacks
├── snapshot.py        — last-known-state cache per symbol
├── topics.py          — venue.channel.product_id parser
├── models.py          — Pydantic envelopes (Ticker, Trade, L2Update, …)
├── logging_config.py  — structlog JSON setup
├── ingest/
│   └── coinbase.py    — Coinbase WS client + reconnect + reconcile
└── server/
    ├── ws.py          — downstream WebSocket server
    └── status.py      — HTTP /status + /healthz

tests/                 — pytest suite (215 tests, all green)
scripts/
├── start.sh           — venv-aware launcher
├── smoke_ws_client.py — minimal WS client for manual verification
└── spike_coinbase.py  — Step 1.5 throwaway (talks directly to Coinbase)

config/
└── default.yaml       — runtime defaults, override via env vars

progress/              — implementation log
docs/                  — (Step 9) context docs for LLM agents
```

---

## Logging

JSON to stdout via [structlog](https://www.structlog.org/). Every event has a
namespaced `event` key (e.g. `ingest.subscribe_sent`, `ws_server.client_connected`),
plus consistent fields where applicable: `consumer_id`, `topic`, `peer`.

```bash
LOG_LEVEL=DEBUG ./scripts/start.sh           # verbose
./scripts/start.sh 2>&1 | jq -c .             # pretty-pipe through jq
./scripts/start.sh 2>&1 | jq -r 'select(.event|startswith("ingest"))'
```

---

## Development

```bash
# Run tests
.venv/bin/python -m pytest -q

# Lint
.venv/bin/python -m ruff check src tests

# Live smoke (assumes hub already running)
.venv/bin/python scripts/smoke_ws_client.py coinbase.ticker.BTC-USD 5
curl -s http://localhost:8080/status | python -m json.tool
```

Architectural conventions are in [CLAUDE.md](CLAUDE.md). Implementation log
with rationale per step in
[progress/20260425_implementation_plan.txt](progress/20260425_implementation_plan.txt).

---

## License

Internal assessment project. Not for distribution.
