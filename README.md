# Market Data Broker

A real-time crypto market-data hub. Ingests from Coinbase, distributes to many
downstream consumers via a single demand-driven pub/sub bus, exposes itself
both over plain WebSocket (for general clients) and over MCP (for LLM agents).

The hub is **demand-driven** — nothing is upstream-subscribed until a consumer
asks for a topic.

---

## Quickstart

Requires Python 3.11+ (Docker works without Python on host).

```bash
python3 -m venv .venv
.venv/bin/pip install -e ".[dev]"
./scripts/start.sh                              # python hub (default)
```

`scripts/start.sh` is the unified launcher. Two axes — **mode** (python or
docker) and **service** (hub or mcp):

| Command | What it runs |
|---|---|
| `./scripts/start.sh` | python hub — WS `:8765` + `/status :8080` |
| `./scripts/start.sh --service mcp` | python mcp — HTTP `:8000` |
| `./scripts/start.sh --mode docker` | docker hub |
| `./scripts/start.sh --mode docker --service mcp` | docker mcp |
| `./scripts/start.sh --mode docker --service all` | docker hub + mcp |
| `./scripts/start.sh --help` | full reference |

Ctrl+C stops; shutdown takes ~1–3s while ingest closes the upstream WS cleanly.

---

## Testing the hub

Three options, depending on what you want to exercise:

**Plain WebSocket client** — exercises the WS server end of the pipeline.

```bash
./scripts/start.sh                                                     # terminal 1
.venv/bin/python scripts/smoke_ws_client.py coinbase.ticker.BTC-USD 5  # terminal 2
```

**MCP Inspector** — UI for poking individual MCP tools.

```bash
./scripts/start.sh --service mcp     # terminal 1: HTTP MCP on :8000
./scripts/mcp_inspector.sh           # terminal 2: opens inspector pre-wired
```

**Claude Desktop** — full LLM-agent end-to-end.

```bash
./scripts/start.sh --service mcp                # terminal 1
./scripts/claude_desktop_setup.sh               # prints config + demo prompts
```

**Multi-client lifecycle** — visualise fan-out + the "no leaked upstream subs"
guarantee. Spawns 3 WS clients across 2 topics, kills them in sequence, and
queries `/status` between each transition.

```bash
./scripts/start.sh                              # terminal 1
./scripts/demo_multi_client.sh                  # terminal 2
```

Operator-side observability:

```bash
curl -s http://localhost:8080/status  | python -m json.tool   # hub state
curl -s http://localhost:8080/healthz                         # liveness ping
```

---

## Topic naming

```
{venue}.{channel}.{product_id}     e.g. coinbase.ticker.BTC-USD
```

| Channel | What it carries | Cadence |
|---|---|---|
| `ticker` | Last trade + post-trade top-of-book | One per trade (~5 Hz on BTC) |
| `matches` | Executed trades | One per trade |
| `level2_batch` | L2 book snapshot then incremental updates | Snapshot once + diffs |

The `venue.` prefix leaves room for adding e.g. `binance.*` later without
schema changes.

---

## Architecture

```
                    Coinbase WSS (one connection)
                              │
                              ▼
                     ┌─────────────────┐
                     │     ingest      │  reconnect, watchdog,
                     └────────┬────────┘  demand reconcile
                              │ Message envelope
                              ▼
                     ┌─────────────────┐
                     │       bus       │  in-memory pub/sub,
                     └────────┬────────┘  drop-oldest backpressure
                              │
                ┌─────────────┴─────────────┐
                ▼ (passive observer)        ▼ (registry-mediated)
          ┌───────────┐                ┌───────────┐
          │ snapshot  │                │ registry  │  ref-counts holders;
          │   cache   │                └─────┬─────┘  drives ingest sub/unsub
          └───────────┘                      │       on 0↔1 edges
                          ┌──────────────────┼──────────────────┐
                          ▼                  ▼                  ▼
                    ┌───────────┐      ┌───────────┐      ┌───────────┐
                    │ server.ws │      │ server.mcp│      │server.    │
                    │  :8765    │      │  :8000    │      │status:8080│
                    └───────────┘      └───────────┘      └───────────┘
                    WS clients         LLM agents          ops / health
```

- **ingest** — owns the Coinbase WS; reconnect with backoff; demand reconcile.
- **bus** — in-memory pub/sub; drop-oldest backpressure with per-consumer counters.
- **registry** — ref-counts subscriptions; drives ingest sub/unsub at 0↔1 transitions. **No leaked upstream subs.**
- **snapshot** — passive cache of last ticker / trade / top-of-book per symbol. Bus-direct (not a registry consumer) so it doesn't create artificial demand.
- **server.ws / server.mcp / server.status** — public surfaces. Each WS / MCP connection is a registry consumer; status is read-only.

The four-layer split (ingest → bus → registry → server surfaces) is the hinge
of the design: adding a venue or a protocol means writing one new module, the
others don't change.

Full design choices, trade-offs, and extension paths in
[docs/architecture.md](docs/architecture.md). LLM-facing context docs in
[docs/](docs/) (`context.md`, `topics.md`, `worked_examples.md`,
`failure_modes.md`).

---

## Configuration

Defaults in [config/default.yaml](config/default.yaml). Override via env vars
— see [.env.example](.env.example). Only
[__main__.py](src/market_data_broker/__main__.py) reads env vars; every other
module takes constructor kwargs.

---

## Restart-state caveats

The hub holds **all state in memory** — no database, no persistence. On restart
the snapshot cache, consumer subscriptions, and msg-rate stats are lost; clients
are responsible for retry/reconnect; upstream Coinbase subscriptions are
reconciled from new downstream demand.

By design — the hub is a streaming pipe, not a store of record.

---

## Development

```bash
.venv/bin/python -m pytest -q              # 239 tests
.venv/bin/python -m ruff check src tests   # lint
LOG_LEVEL=DEBUG ./scripts/start.sh         # verbose structlog JSON
```

Conventions: [CLAUDE.md](CLAUDE.md). Implementation log with rationale per step:
[progress/20260425_implementation_plan.txt](progress/20260425_implementation_plan.txt).

---

## License

Internal assessment project. Not for distribution.
