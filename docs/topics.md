# Topics

A **topic** is the routing key for everything that flows through the hub.
Subscribe to a topic; the hub streams `Message` envelopes carrying that
topic's payload schema until you unsubscribe.

## Naming

```
{venue}.{channel}.{product_id}
```

| Part | Format | Example |
|---|---|---|
| `venue` | lowercase, alphanumeric + underscore | `coinbase` |
| `channel` | lowercase, alphanumeric + underscore | `ticker`, `matches`, `level2_batch` |
| `product_id` | uppercase, single hyphen | `BTC-USD`, `ETH-USD`, `SOL-USD` |

Full topic examples: `coinbase.ticker.BTC-USD`, `coinbase.matches.ETH-USD`,
`coinbase.level2_batch.SOL-USD`.

The `venue.` prefix leaves room for a future `binance.*` ingest; existing
topic names won't change.

## The Message envelope

Every frame on the wire is wrapped in this envelope:

```json
{
  "topic": "coinbase.ticker.BTC-USD",
  "venue": "coinbase",
  "received_at": "2026-04-25T13:53:06.171000Z",
  "payload": { ... }
}
```

- `topic` — the routing key (matches your subscription).
- `venue` — convenience copy of the venue prefix.
- `received_at` — ISO-8601 UTC, when the hub *received* the frame from the
  upstream venue. Useful for staleness checks.
- `payload` — the channel-specific body. Type discriminator inside is
  `payload.type`.

## Channels

### `ticker` — one frame per trade with post-trade top-of-book

**When to use:** you want the latest price *and* the top of the book updated
together on every trade. The most useful single channel.

**Cadence:** one frame per trade. ~5 Hz on `BTC-USD` during US market hours,
much lower on illiquid pairs.

**Payload schema:**

| Field | Type | Notes |
|---|---|---|
| `type` | `"ticker"` | discriminator |
| `product_id` | string | e.g. `"BTC-USD"` |
| `sequence` | int | monotonic per product; useful for gap detection |
| `trade_id` | int | unique per trade |
| `time` | string (ISO-8601 UTC) | when Coinbase printed the trade |
| `price` | **string** (Decimal) | trade price |
| `last_size` | **string** (Decimal) | trade size |
| `side` | `"buy"` \| `"sell"` | taker side |
| `best_bid` | **string** (Decimal) | post-trade |
| `best_bid_size` | **string** (Decimal) | post-trade |
| `best_ask` | **string** (Decimal) | post-trade |
| `best_ask_size` | **string** (Decimal) | post-trade |
| `open_24h` / `high_24h` / `low_24h` / `volume_24h` / `volume_30d` | **strings** (Decimal) | rolling stats |

**Real example payload** (captured against live Coinbase):

```json
{
  "type": "ticker",
  "product_id": "BTC-USD",
  "sequence": 127113500279,
  "trade_id": 1008094628,
  "time": "2026-04-25T13:53:06.170361Z",
  "price": "77668.92",
  "side": "buy",
  "last_size": "0.0003128",
  "best_bid": "77668.91",
  "best_bid_size": "0.20495031",
  "best_ask": "77668.92",
  "best_ask_size": "0.49546688",
  "open_24h": "78107.84",
  "high_24h": "78257.09",
  "low_24h": "77289",
  "volume_24h": "5457.73586321",
  "volume_30d": "261517.66959213"
}
```

> **Mid price** = (best_bid + best_ask) / 2. Use Decimal arithmetic; do not
> parse to float.

### `matches` — one frame per executed trade

**When to use:** you only care about trades, not the book. Lighter than
`ticker` (no rolling stats, no top-of-book bookkeeping in the frame).

**Cadence:** one frame per trade.

**Payload schema:**

| Field | Type | Notes |
|---|---|---|
| `type` | `"match"` \| `"last_match"` | `last_match` only on the *first* frame after subscribe (catch-up); subsequent frames are `match`. Same fields. |
| `product_id` | string | |
| `sequence` | int | monotonic per product |
| `trade_id` | int | |
| `time` | ISO-8601 UTC | |
| `price` | **string** (Decimal) | |
| `size` | **string** (Decimal) | |
| `side` | `"buy"` \| `"sell"` | taker side |
| `maker_order_id` | string (UUID) | |
| `taker_order_id` | string (UUID) | |

**Real example payload:**

```json
{
  "type": "last_match",
  "product_id": "BTC-USD",
  "sequence": 127113513369,
  "trade_id": 1008094908,
  "time": "2026-04-25T13:53:49.062102Z",
  "price": "77706.48",
  "size": "0.0000001",
  "side": "buy",
  "maker_order_id": "13de9002-7b7f-4728-a51a-eb59ccbbcd43",
  "taker_order_id": "a68a6f59-4d2c-4b01-8656-4147e24e89cf"
}
```

### `level2_batch` — full order book

**When to use:** you need depth, not just top of book. Aware of the
on-subscribe cost.

**Cadence:**
- ONE `snapshot` frame on subscribe (~1 MB; top 50 levels per side)
- Continuous `l2update` frames after that (small)

**Snapshot schema:**

```json
{
  "type": "snapshot",
  "product_id": "BTC-USD",
  "asks": [["77707.86", "0.34855"], ["77708.10", "0.50"], "..."],
  "bids": [["77707.85", "0.20"], ["77707.50", "1.20"], "..."]
}
```

- `asks` ordered by price ascending; `bids` descending. (The hub doesn't
  enforce ordering — sort if you need a guarantee.)
- Each entry is `[price, size]`. Both are **strings** (Decimal).

> Coinbase's `level2_batch` is server-aliased to `level2_50` — you get the
> top 50 levels per side, not full depth. Plenty for top-of-book queries.

**Update schema:**

```json
{
  "type": "l2update",
  "product_id": "BTC-USD",
  "time": "2026-04-25T13:54:01.123456Z",
  "changes": [
    ["buy",  "77697.01", "0.13838760"],
    ["buy",  "77647.43", "0.00000000"]
  ]
}
```

- Each change is `[side, price, size]`.
- `size: "0"` means **remove that price level** from the book.
- The hub does not maintain the book — it just forwards updates. The snapshot
  store reads the initial `snapshot` for best bid/ask but ignores
  `l2update` (only `last_update_at` ticks).

## Symbols

The hub doesn't ship a fixed symbol list — anything Coinbase publishes can
become a topic if a consumer subscribes. Common Coinbase symbols you'll
encounter:

| Symbol | Description |
|---|---|
| `BTC-USD` | Bitcoin / US dollar |
| `ETH-USD` | Ether / US dollar |
| `SOL-USD` | Solana / US dollar |
| `XRP-USD` | XRP / US dollar |
| `LINK-USD` | Chainlink / US dollar |

Format: `{base}-{quote}`, both uppercase, single hyphen. Stablecoin pairs
typically use `-USDT` or `-USDC` instead of `-USD`.

If you're unsure whether Coinbase trades a particular pair, the hub can't
tell you — it's a streaming hub, not a product catalogue. The
authoritative source is Coinbase's own products list. Subscribing to a
non-existent symbol is silently ignored upstream (see
[failure_modes.md](failure_modes.md)).

## Numeric handling — read this once

**Every numeric field is a JSON string.** Examples:

```json
{ "price": "77668.92", "last_size": "0.0003128" }
```

This is intentional. Coinbase sends them as strings; the hub preserves them
as strings to avoid float rounding errors at BTC-scale notionals.

To compute, convert to a Decimal-equivalent:

| Language | What to use |
|---|---|
| Python | `decimal.Decimal(...)` |
| TypeScript | `decimal.js`, `bignumber.js`, or BigInt for whole-cents math |
| Rust | `rust_decimal::Decimal` |
| Go | `shopspring/decimal` |

Never parse to a binary float (`float`, `f64`, `number` in TS) for any code
that *touches money*. Display-only formatting is fine.
