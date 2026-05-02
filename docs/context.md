# Context — Market Data Broker

> This doc is for an LLM agent (or any new consumer) cold-starting against
> the hub. It tells you what the hub is, what you can and cannot ask, and
> where to look next.

## What the hub is

A real-time crypto market-data hub. It maintains a live WebSocket to
Coinbase, normalises the data into typed messages, and serves it to many
downstream consumers — including LLM agents, internal services, and ad-hoc
WS clients.

You can ask the hub:

- **What is currently available** — list of topics + their schemas.
- **The latest known state for a symbol** — last trade, best bid, best ask.
- **A short live stream of recent messages** — capped, polled.
- **The hub's own health and statistics** — uptime, upstream state, message
  rates, drop counts.

You **cannot** ask the hub:

- For historical data ("BTC price one hour ago" — it isn't stored).
- To place or cancel orders (read-only, no auth, no trading API).
- For symbols Coinbase doesn't publish (the hub is a passthrough).
- For symbols across venues other than Coinbase (Binance etc. are roadmap,
  not present).

## How to interact

Two separate surfaces; pick whichever fits the caller:

| Surface | Best for | Endpoint |
|---|---|---|
| **MCP** | LLM agents, Claude Desktop | tools over stdio (`mcp --stdio`) or streamable-HTTP (`mcp --http`) |
| **WebSocket** | Internal services, browser code, scripts | `ws://hub:8765` |
| **HTTP `/status`** | Operators, container health checks | `http://hub:8080/status` |

The MCP surface is what an LLM agent normally uses. Tools:

- `list_topics()` — enumerate available topics with cadence + venue + channel.
- `describe_topic(topic)` — full schema and a real example payload.
- `get_snapshot(product_id)` — last known ticker / trade / best bid-ask.
- `get_hub_status()` — same data as the HTTP `/status` endpoint.
- `stream_topic(topic, max_messages, timeout_ms)` — drain N messages or until
  timeout. Caller polls; the hub does not push.

## Topics (the routing key for everything)

Format: `{venue}.{channel}.{product_id}`

Examples:
- `coinbase.ticker.BTC-USD` — BTC ticker stream
- `coinbase.matches.ETH-USD` — ETH executed-trade stream
- `coinbase.level2_batch.SOL-USD` — SOL order-book stream

`venue` is always `coinbase` today. `product_id` is uppercase, hyphenated:
`BTC-USD`, never `btc-usd` or `BTCUSD`. Full schemas in [topics.md](topics.md).

## Three things to remember

1. **Numerics are strings, not numbers.** Prices and sizes come through as
   `"77668.92"` to preserve full Decimal precision. Don't parse to float for
   arithmetic — convert to a Decimal/BigDecimal type.

2. **The hub is demand-driven.** It only subscribes to Coinbase for topics
   that have at least one downstream consumer. If you ask about a topic
   nobody has subscribed to yet, `get_snapshot` returns null. Use
   `stream_topic` (with a tiny `max_messages`) to "warm up" a topic before
   querying its snapshot.

3. **State is in-memory only.** No database, no persistence. After a hub
   restart, snapshots are empty until the first frames flow on each topic.

## Where to look next

- [topics.md](topics.md) — every topic's schema, cadence, real example payload.
- [failure_modes.md](failure_modes.md) — what you see when something goes
  wrong (stale data, hub reconnecting, unknown symbol, etc.) and how to handle it.
- [worked_examples.md](worked_examples.md) — end-to-end recipes
  ("get current mid for BTC-USD" → tool calls → answer).
- [architecture.md](architecture.md) — for engineers; design write-up and
  extension paths.

## What "scope" means here

This hub is **single-venue (Coinbase), three-channel (ticker / matches /
level2_batch), in-memory, single-process**. If a request asks for anything
outside that envelope (e.g. historical bars, order placement, multi-venue
analytics), say so explicitly rather than guessing — the hub will return
null/empty/error and the agent should not retry indefinitely.
