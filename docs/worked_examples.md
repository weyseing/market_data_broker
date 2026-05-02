# Worked Examples

Concrete recipes an LLM agent (or any consumer) can follow end-to-end. Each
example shows: **what the user asked**, **which tools to call in what order**,
**what the responses look like**, and **how to compose the final answer**.

These walkthroughs use the MCP tool set described in [context.md](context.md)
and [topics.md](topics.md). The same flows work over the WebSocket protocol —
just substitute the relevant subscribe/unsubscribe frames.

---

## Example 1 — "What is the current mid for BTC-USD?"

The most common query: a single number, right now.

### The agent's reasoning

> Mid = (best_bid + best_ask) / 2. The hub publishes top-of-book on the
> `ticker` channel. I want the latest cached value, not a stream — so use
> `get_snapshot`, not `stream_topic`.

### Step 1 — try the cache

```json
// → tool call
{"name": "get_snapshot", "arguments": {"product_id": "BTC-USD"}}
```

If the topic is already active (someone else has subscribed recently), this
returns immediately:

```json
{
  "product_id": "BTC-USD",
  "found": true,
  "snapshot": {
    "product_id": "BTC-USD",
    "last_ticker": {"price": "77668.92", ...},
    "best_bid": "77668.91",
    "best_bid_size": "0.20495031",
    "best_ask": "77668.92",
    "best_ask_size": "0.49546688",
    "last_update_at": "2026-04-25T13:53:06.171000Z"
  }
}
```

### Step 2 — compute the mid

```python
from decimal import Decimal
mid = (Decimal("77668.91") + Decimal("77668.92")) / 2
# Decimal("77668.915")
```

> **Always Decimal, never float.** The strings exist precisely to avoid
> float rounding in price math.

### Step 3 — sanity check staleness

`last_update_at` was milliseconds ago — fresh. Reply to the user:

> The current mid for BTC-USD is **$77,668.915** (best bid $77,668.91 / best
> ask $77,668.92, updated less than a second ago).

### What if `found` is `false`?

The topic was never subscribed, or the hub just restarted. Warm the cache:

```json
{"name": "stream_topic",
 "arguments": {"topic": "coinbase.ticker.BTC-USD",
               "max_messages": 1, "timeout_ms": 5000}}
```

That registers a transient consumer, ingest sends an upstream subscribe, and
the snapshot store starts tracking. After it returns (1 message in, typically
<1s), retry `get_snapshot` — it will now have the cached top-of-book.

If `stream_topic` returns `received: 0` after 5 seconds, the symbol probably
isn't traded on Coinbase. Don't loop — tell the user.

---

## Example 2 — "Sample 5 recent BTC-USD trades."

The user wants observed trades, not a price. The right channel is `matches`.

### Step 1 — discover the topic shape (optional)

If the agent isn't sure what `matches` carries, look it up:

```json
{"name": "describe_topic",
 "arguments": {"topic": "coinbase.matches.BTC-USD"}}
```

Returns the schema and a real example payload. Useful before subscribing if
the agent will need to format fields it hasn't seen.

### Step 2 — drain 5 frames

```json
{"name": "stream_topic",
 "arguments": {"topic": "coinbase.matches.BTC-USD",
               "max_messages": 5, "timeout_ms": 10000}}
```

The first message after subscribe is `type: "last_match"` (catch-up of the
most recent trade), the rest are `type: "match"` (live). Both have identical
fields.

```json
{
  "topic": "coinbase.matches.BTC-USD",
  "received": 5,
  "messages": [
    {"topic": "...", "payload": {"type": "last_match", "price": "77706.48", "size": "0.0000001", "side": "buy", ...}},
    {"topic": "...", "payload": {"type": "match",       "price": "77707.00", "size": "0.01",      "side": "sell", ...}},
    ...
  ]
}
```

### Step 3 — present

```
1. 0.0000001 BTC @ $77,706.48 (buy)   — catch-up
2. 0.01      BTC @ $77,707.00 (sell)
...
```

### What if `received < max_messages`?

Either the pair is illiquid (no trades in 10s — fine, report what arrived),
or the upstream feed is silent. Call `get_hub_status` to disambiguate:

- `upstreams[].state == "connected"` and `topics[].messages_total` > 0 → just
  low activity.
- `state == "reconnecting"` → upstream is flapping; surface that to the user.

---

## Example 3 — "Is BTC moving up or down right now?"

Compute a direction from a short stream.

### Step 1 — sample 20 ticker frames

```json
{"name": "stream_topic",
 "arguments": {"topic": "coinbase.ticker.BTC-USD",
               "max_messages": 20, "timeout_ms": 8000}}
```

### Step 2 — compare first vs last price

```python
prices = [Decimal(m["payload"]["price"]) for m in result["messages"]]
delta  = prices[-1] - prices[0]
window = "across the last ~4 seconds of trading"
direction = "up" if delta > 0 else "down" if delta < 0 else "flat"
```

### Step 3 — qualify

Always include the sample size and time window — a 20-frame look is
information about *the last few seconds*, not "the trend." The user can
re-ask with a larger `max_messages` if they want a longer window. Cap is
5000 messages or 60 seconds, whichever comes first.

> BTC is **down** $1.23 across the last 20 trades (~3.4 seconds).

---

## Example 4 — "Is the hub healthy?"

Operational, not market-data. Use `get_hub_status` directly.

```json
{"name": "get_hub_status", "arguments": {}}
```

```json
{
  "hub_uptime_seconds": 12345.6,
  "upstreams": [
    {
      "name": "coinbase",
      "state": "connected",
      "reconnect_count": 0,
      "topics": ["coinbase.ticker.BTC-USD"]
    }
  ],
  "consumers": [
    {"consumer_id": "ws-3", "topics": ["coinbase.ticker.BTC-USD"],
     "queue_size": 0, "dropped_messages": 0}
  ],
  "topics": [
    {"topic": "coinbase.ticker.BTC-USD",
     "subscriber_count": 1,
     "messages_total": 41203,
     "messages_per_second": 3.34}
  ]
}
```

### What "healthy" looks like

- `upstreams[].state == "connected"`
- `upstreams[].reconnect_count` is small and not growing across calls
- `topics[].messages_per_second` > 0 for any topic with subscribers (during
  market hours)
- `consumers[].dropped_messages` is 0 (or stable, not climbing)

### What "unhealthy" looks like

- `state: "reconnecting"` for more than ~30 seconds → upstream is flapping.
- `dropped_messages` climbing on a specific consumer → that consumer is too
  slow. WebSocket clients past `bus.sustained_overflow_drops` will be
  disconnected with close code 1013.
- All `messages_total` at 0 minutes after `subscriber_count` went above 0 →
  ingest is connected but Coinbase isn't sending. The watchdog forces a
  reconnect after `heartbeat_timeout_seconds` (default 15s).

See [failure_modes.md](failure_modes.md) for the full diagnosis tree.

---

## Example 5 — "What can I subscribe to?" (cold start)

A new agent with no context.

### Step 1 — discover the taxonomy

```json
{"name": "list_topics", "arguments": {}}
```

```json
{
  "venues": ["coinbase"],
  "channels": ["level2_batch", "matches", "ticker"],
  "topic_format": "{venue}.{channel}.{product_id}",
  "examples": ["coinbase.ticker.BTC-USD", "coinbase.matches.ETH-USD",
               "coinbase.level2_batch.SOL-USD"],
  "active_topics": ["coinbase.ticker.BTC-USD"],
  "upstream_active_topics": ["coinbase.ticker.BTC-USD"],
  "note": "The hub is demand-driven: a topic only exists in 'active_topics' while a downstream consumer wants it. Any valid {venue}.{channel}.{product_id} string is subscribable, even if not currently active."
}
```

### Step 2 — pick a channel

The user wanted "the latest price" → `ticker`. They wanted "trades" → `matches`.
They wanted "the order book" → `level2_batch`. See `describe_topic` for the
schema of each.

### Step 3 — build the topic string

`{venue}.{channel}.{product_id}` — venue always `coinbase`, channels from the
list above, `product_id` uppercase with a single hyphen
(`BTC-USD`, never `btc-usd` or `BTCUSD`). The hub doesn't validate symbols
exist on Coinbase — see [failure_modes.md § Unknown symbol](failure_modes.md#1-unknown--unsupported-symbol).

---

## Anti-patterns

Things that look reasonable but waste the hub's time (or yours):

| Don't | Do instead |
|---|---|
| Loop `get_snapshot` 50 times waiting for a value to change | Call `stream_topic` once with `max_messages=N` and look at deltas |
| Subscribe to `level2_batch` to get best bid/ask | Use `ticker` — same data, no 1 MB snapshot transfer |
| Parse `price` to `float` | Use `Decimal` — float will lose cents at BTC-scale |
| Retry on `found: false` immediately | Call `stream_topic` to warm the cache, then retry once |
| Subscribe to `coinbase.heartbeat.BTC-USD` (looks like a real channel) | Use a supported channel — `heartbeat` is filtered upstream |

---

## Cheat sheet

```
"latest price"          → get_snapshot
"recent trades"         → stream_topic on matches
"is it moving"          → stream_topic on ticker, compare first/last
"is the hub OK?"        → get_hub_status
"what's available?"     → list_topics
"what does X look like?"→ describe_topic
```

When in doubt, `get_hub_status` first. If the hub is healthy and you still
get nothing, the topic is the problem; if the hub is reconnecting, wait.
