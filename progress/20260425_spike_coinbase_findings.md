# Coinbase WS Spike — Findings

Date: 2026-04-25
Source: `scripts/spike_coinbase.py` against `wss://ws-feed.exchange.coinbase.com`
Purpose: confirm real message shapes and surface protocol gotchas before locking
in Pydantic models (Step 2) and writing the production ingest client (Step 5).

## Endpoint

- URL: `wss://ws-feed.exchange.coinbase.com` (Coinbase Exchange public feed).
- No auth required for `ticker`, `matches`, `level2_batch`, `heartbeat`.
- Subscribe frame: `{"type":"subscribe","product_ids":[...],"channels":[...]}`.
- Server replies with a `"subscriptions"` frame echoing what was actually subscribed
  (see level2 aliasing below).

## Channel: ticker

Subscription confirms with `name: "ticker"`. Update frame:

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

Notes:
- All numerics are **strings** — preserve as `Decimal` in models, never `float`.
- `time` is RFC3339 / ISO8601 UTC.
- `sequence` is monotonic per product; useful for gap detection.

## Channel: matches

Subscription confirms with `name: "matches"`. Update frame:

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

Notes:
- Type discriminator: `"last_match"` for the initial frame after subscribe,
  `"match"` for subsequent live trades. Models must accept both.
- `side` is the taker side.

## Channel: level2_batch — TWO important gotchas

### Gotcha 1: server aliases the channel name

Subscribed with `"channels":["level2_batch"]`. Server reply:

```json
{ "type": "subscriptions",
  "channels": [{"name": "level2_50", "product_ids": ["BTC-USD"], ...}] }
```

The exchange now serves a curated **top-50-levels** feed (`level2_50`) instead of
the historical full-depth `level2`. Implications:

- We get top-of-book + 50 levels per side, not full depth — fine for our use-case
  (snapshot store needs best bid/ask).
- Topic naming: keep external topic name `coinbase.level2_batch.{product_id}` for
  consistency with the other channels and the plan's stated scope, but document
  in `docs/topics.md` (Step 9) that the underlying feed is `level2_50`.

### Gotcha 2: snapshot frame exceeds default `max_size`

The first frame after subscribing is a **full snapshot** approaching ~1 MB. The
`websockets` library defaults to `max_size=2**20` (1 MiB) per frame, which causes
the connection to be torn down with close code **1009 (MESSAGE_TOO_BIG)**:

```
ConnectionClosedError: sent 1009 frame after reading 1036856 bytes exceeds
limit of 1048576 bytes
```

**Action for Step 5**: the production WS client must set `max_size=None` (or a
generous limit, e.g. 4 MiB) when connecting. Without this fix, level2 subscriptions
silently fail at startup.

### Frame shapes

Snapshot (one per subscription, large):

```json
{
  "type": "snapshot",
  "product_id": "BTC-USD",
  "asks": [["77707.86", "0.34855132"], ...],
  "bids": [["77707.85", "0.20111111"], ...]
}
```

Update (streamed continuously):

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

Notes:
- `changes[i] = [side, price, size]`. `size == "0"` means remove that level.
- Snapshot has no `time`; updates do.
- Snapshot lacks `sequence` and `trade_id` (book state, not trade state).

## Cross-cutting points for the production code

1. **Connect with `max_size=None`** — required for level2.
2. **Heartbeat / liveness**: `websockets.connect(ping_interval=20, ping_timeout=20)`
   gave clean idle behavior in the spike. Step 5 should also subscribe to the
   `heartbeat` channel per product to detect dead feeds even when no trades print.
3. **All numbers are strings** — never coerce to `float`; use `Decimal` end-to-end.
4. **Discriminator field is `type`** — drives the Step 2 `Message` envelope union.
5. **`product_id` always present** on data frames — clean topic key.
6. **`sequence` present on ticker + matches** — drop or warn on backwards/skip.

## Spike script

Kept at `scripts/spike_coinbase.py` for ad-hoc re-runs. Not part of the runtime
package. Usage:

```
python scripts/spike_coinbase.py --channel ticker --max 3
python scripts/spike_coinbase.py --channel matches --products BTC-USD,ETH-USD
python scripts/spike_coinbase.py --channel level2_batch --max 2
```
