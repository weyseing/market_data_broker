# Architecture

One-page write-up of the design. For implementation status and rationale per
step, see [progress/20260425_implementation_plan.txt](../progress/20260425_implementation_plan.txt).

## One picture

```
                     Coinbase WSS (one connection)
                              │
                              ▼
                   ┌─────────────────────┐
                   │  ingest.coinbase    │  reconnect, watchdog, demand reconcile
                   └──────────┬──────────┘
                              │ Message envelope
                              ▼
                   ┌─────────────────────┐
                   │       bus           │  in-memory pub/sub, drop-oldest
                   └──┬───────┬──────────┘     backpressure per consumer
                      │       │
       ┌──────────────┘       └──────────────┐
       │ (passive observer)                  │ (registry-mediated)
       ▼                                     ▼
┌────────────┐                       ┌─────────────────┐
│ snapshot   │                       │    registry     │
│   store    │                       │ ref-counts      │
└────────────┘                       │ topic holders;  │
                                     │ fires on_first /│
                                     │ on_last → drives│
                                     │ ingest sub/unsub│
                                     └────┬────────────┘
                                          │
              ┌───────────────────────────┼───────────────────────────┐
              ▼                           ▼                           ▼
      ┌───────────────┐           ┌────────────────┐         ┌────────────────┐
      │ server.ws     │           │ server.status  │         │ server.mcp     │
      │ (port 8765)   │           │ (port 8080)    │         │ stdio + http   │
      │ WS clients    │           │ /status,       │         │ LLM agents     │
      │               │           │ /healthz       │         │ (Step 8)       │
      └───────────────┘           └────────────────┘         └────────────────┘
```

## Layers and responsibilities

| Layer | File(s) | Responsibility |
|---|---|---|
| **Ingestion** | [ingest/coinbase.py](../src/market_data_broker/ingest/coinbase.py) | Maintain one WS to Coinbase; subscribe upstream on demand; reconnect with exp backoff; stale-feed watchdog; parse + publish to bus |
| **Bus** | [bus.py](../src/market_data_broker/bus.py) | Route `Message` envelopes to subscriber queues. Bounded queue per consumer; drop-oldest on overflow |
| **Registry** | [registry.py](../src/market_data_broker/registry.py) | Single source of truth for consumer subscriptions. Ref-counts topic holders; fires `on_first_subscriber` / `on_last_unsubscriber` callbacks that ingest hooks into. Surfaces `status()` |
| **Snapshot** | [snapshot.py](../src/market_data_broker/snapshot.py) | Cache last ticker / trade / best bid-ask per `product_id`. Bus-direct subscriber — *not* via registry |
| **WS server** | [server/ws.py](../src/market_data_broker/server/ws.py) | Downstream WebSocket fan-out. Each connection is a registry consumer |
| **Status server** | [server/status.py](../src/market_data_broker/server/status.py) | HTTP `/status` (full registry state) + `/healthz` (liveness) |
| **MCP server** (Step 8) | server/mcp_server.py | Wrap the registry + snapshot in MCP tools for LLM agents |

The four-layer split (ingest → registry → bus → server surfaces) is the
hinge of the design. Adding a venue or a protocol means writing one new
module; the others don't change.

## Key design choices and trade-offs

### 1. Demand-driven upstream

Reviewers called out leaked upstream subscriptions as a primary failure mode.
We solve it with **ref-counting in the registry**: a topic transitions
0 → 1 → 0 holders, and the registry fires callbacks at each edge. Ingest
listens and emits SUBSCRIBE / UNSUBSCRIBE frames upstream.

- **Win:** zero residual upstream subs after the last consumer leaves.
  Verified by `test_disconnect_releases_topics_no_leak`.
- **Trade-off:** every consumer must go through the registry — bus subscriptions
  on their own would bypass the ref-count. Pinned by
  `test_snapshot_store_does_not_create_registry_demand` (snapshot store is the
  one intentional exception, and it's bus-direct).

### 2. Snapshot store is *not* a registry consumer

The snapshot cache subscribes to the bus directly, never via the registry.
If it went through the registry, every snapshotted topic would have an
artificial holder, ingest would never unsubscribe upstream, and the
demand-driven property would silently break.

Instead, the snapshot store's `track(topic)` / `untrack(topic)` is composed
into the registry callbacks in [\_\_main\_\_.py](../src/market_data_broker/__main__.py)
so it sees a topic exactly while there's real downstream demand — without
*creating* demand.

### 3. Single bus, single ingest task, single registry

Process-local, single-event-loop asyncio. No locks, no queues between tasks
beyond the bus itself.

- **Win:** simple to reason about. The bus's publish is atomic w.r.t. other
  coroutines; no race conditions to engineer around.
- **Trade-off:** no horizontal scaling. For the assessment scope this is
  fine; in production you'd shard by symbol with a message-broker
  in front.

### 4. Backpressure: drop-oldest + cumulative cap

Each consumer's queue is bounded (default 1000). On overflow we drop the
*oldest* message — a slow consumer cannot stall publishers or peers, but its
own backlog gets trimmed.

A second guard (cumulative drop cap, default 100) closes the connection with
WS code 1013 if the drops keep accumulating. This prevents a slow consumer
from streaming stale data forever.

### 5. In-memory only, no persistence

The hub is a streaming pipe, not a store of record. State lost on restart:
snapshot cache, consumer subscriptions, registry stats. Recovery is automatic
as new messages flow.

- **Win:** zero ops complexity. No DB, no migrations, no backup story.
- **Trade-off:** caches take a few seconds to warm after restart. Documented
  in [README](../README.md) and [failure_modes.md](failure_modes.md).

### 6. Layered server surfaces (WS + HTTP + MCP)

Each consumer surface is a thin adapter over the registry:

- **WS server** translates wire frames into `registry.add_topic` /
  `remove_topic` calls.
- **MCP server** (Step 8) will translate tool calls into the same registry
  calls plus snapshot reads.
- **Status HTTP** is read-only — it just serialises `registry.status()`.

Adding a new surface (e.g. gRPC) means writing one new adapter; the bus,
registry, ingest, and snapshot don't change.

## Extension paths

### Adding a new venue (e.g. Binance)

1. New module: `src/market_data_broker/ingest/binance.py`. Same shape as
   `CoinbaseIngest`: own its WS, expose `on_first_subscriber` /
   `on_last_unsubscriber`, publish normalised `Message` envelopes to the bus.
2. Topic format: `binance.{channel}.{symbol}`. The registry, bus, and
   snapshot store are venue-agnostic — they treat topic strings as opaque keys.
3. Wire into [\_\_main\_\_.py](../src/market_data_broker/__main__.py) alongside
   Coinbase ingest. Both ingests subscribe to registry callbacks; the registry's
   `_parse_coinbase_topic` helper pattern (filter by venue prefix, ignore
   non-yours) generalises trivially.
4. Update topic catalog (Step 8) to include the new venue's channels.

No changes to bus, registry, snapshot store, WS server, status server, or
MCP server (other than the catalog).

### Adding a new consumer surface (e.g. gRPC streaming)

1. New module: `src/market_data_broker/server/grpc_server.py`.
2. Each connection becomes a registry consumer: call `register_consumer`,
   `add_topic`/`remove_topic` on subscribe/unsubscribe, async-iterate the
   bus subscription, fan out to the gRPC stream.
3. Wire start/stop into `__main__.py` mirroring `DownstreamWSServer`.

The "no leaks on disconnect" guarantee carries over for free because it's
implemented in the registry, not in any specific server module.

## What's deliberately not in the design

- **No order book reconstruction beyond top-of-book.** L2 snapshots populate
  best bid/ask once on subscribe; incremental updates only refresh
  `last_update_at`. Building an actively maintained book is straightforward
  but out of scope.
- **No authentication.** WS and MCP both assume a trusted internal network.
  Adding auth means a middleware layer in the server modules; bus/registry
  unaffected.
- **No multi-process scale-out.** Single asyncio loop, single Python process.
  See trade-off (3) above.
