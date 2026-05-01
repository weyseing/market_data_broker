# Failure Modes

What an LLM agent (or any consumer) sees when something goes wrong, and how
to handle each case. If a request looks like it should work but doesn't,
this is the table to consult before retrying or escalating.

---

## 1. Unknown / unsupported symbol

**You did:** subscribed to a topic for a symbol Coinbase doesn't trade
(e.g. `coinbase.ticker.FAKECOIN-USD`).

**You see:**
- The hub accepts the subscribe and forwards it upstream
- Coinbase silently ignores (no error frame)
- No data ever arrives on that topic
- `get_snapshot(product_id="FAKECOIN-USD")` returns null
- `get_hub_status()` shows `subscriber_count: 1` for that topic but
  `messages_total: 0`

**Action:**
1. Verify the symbol against Coinbase's product catalogue (out-of-band — the
   hub doesn't carry that list).
2. Don't retry blindly. Wait several seconds; if `messages_total` is still 0,
   give up and tell the user the symbol is unknown.

---

## 2. Snapshot exists but is stale

**You did:** called `get_snapshot(product_id="BTC-USD")` and got a non-null
result, but `last_update_at` is hours old.

**Why:** somebody subscribed earlier, the hub got data, then the topic went
idle (last subscriber disconnected). Cached snapshot is preserved but no
fresh frames are flowing.

**You see:**
```json
{
  "product_id": "BTC-USD",
  "best_bid": "77668.91",
  "last_update_at": "2026-04-25T11:00:00Z",
  ...
}
```
Where `last_update_at` is much earlier than "now."

**Action:**
- If the user wants live data, call `stream_topic("coinbase.ticker.BTC-USD",
  max_messages=1, timeout_ms=5000)` first. That re-creates demand, ingest
  re-subscribes upstream, and the cache repopulates.
- If the user only wants "the last known price" and is OK with staleness,
  return the cached value — but **always include `last_update_at`** so they
  can judge.

---

## 3. Hub is reconnecting to Coinbase

**You see:** `get_hub_status()` returns:

```json
{
  "upstreams": [
    {
      "name": "coinbase",
      "state": "reconnecting",
      "reconnect_count": 2,
      "last_event_at": "2026-04-25T13:00:00Z",
      "topics": []
    }
  ]
}
```

**Why:** the upstream WS dropped (network blip, Coinbase rotated a server,
stale-feed watchdog forced a close). Hub is in exponential-backoff sleep
between retries.

**You see during the gap:**
- No new data arrives on any topic
- `get_snapshot` still returns whatever was last cached

**Action:**
- Wait. Reconnect typically completes within 30 seconds (max backoff is 30s).
- If `state` flips back to `"connected"` and `reconnect_count` was bumped,
  service is restored — fresh frames will start flowing on subscribed topics.
- If `reconnect_count` keeps climbing without recovery, surface that to the
  user as "the upstream feed is unstable" — don't pretend everything is fine.

---

## 4. Stale feed (connected but silent)

**You see:** `state: "connected"` in `get_hub_status()`, but no frames are
arriving on a topic you subscribed to. After ~15 seconds, hub logs
`ingest.stale_feed_reconnect` and `state` flips to `"reconnecting"`.

**Why:** the underlying TCP is alive but Coinbase isn't pushing. Could be a
liquidity gap on an illiquid pair, or a Coinbase-side issue.

**Action:**
- For known-busy pairs (`BTC-USD`, `ETH-USD`): if frames pause for >15s, the
  watchdog will reconnect automatically. Wait for it.
- For illiquid pairs: silence is normal. The watchdog only kicks in if
  `_desired` is non-empty — i.e. someone is actively expecting frames.

---

## 5. Hub restart

**You did:** the hub process restarted (deploy, crash, manual SIGINT).

**You see:**
- All cached snapshots are gone
- All consumer subscriptions are dropped (clients must reconnect)
- `get_hub_status()` shows `hub_uptime_seconds < 60`, all message counters at 0
- Upstream subscriptions reconcile from new downstream demand

**Action:**
- Re-subscribe.
- Don't trust `get_snapshot` immediately — it'll be empty until the first
  frames flow.
- If you cached snapshot data on your side from before the restart, treat
  it as definitively stale — the symbol's price may have moved.

---

## 6. Slow consumer dropped

**You see (WebSocket clients):** an error frame `{"kind":"error",
"code":"backpressure_exceeded", ...}` followed by the connection closing
with WebSocket close code 1013 ("Try Again Later").

**Why:** you weren't reading messages fast enough. The bus dropped the
oldest messages from your queue and the cumulative drop count crossed the
configured threshold.

**Action:**
1. Reconnect.
2. Make sure the receive loop on your side is non-blocking — every message
   handler should hand off to a worker, not block.
3. If your workload genuinely needs a deeper queue, increase
   `bus.consumer_queue_size` (default 1000) on the hub side.

For MCP `stream_topic` calls this isn't a concern — the call returns within
`timeout_ms` regardless.

---

## 7. Invalid topic format

**You did:** subscribed to a string that doesn't match
`{venue}.{channel}.{product_id}`.

**You see (WS):** `{"kind":"error","code":"invalid_topic","topic":"BAD","message":"..."}`.
Other valid topics in the same batch still apply.

**You see (MCP, when implemented):** the tool call returns an error response
naming the malformed topic.

**Common mistakes:**

| Wrong | Right |
|---|---|
| `coinbase.ticker.btc-usd` | `coinbase.ticker.BTC-USD` (uppercase product) |
| `coinbase.ticker.BTCUSD` | `coinbase.ticker.BTC-USD` (single hyphen required) |
| `coinbase.ticker` | `coinbase.ticker.BTC-USD` (product_id is required) |
| `BINANCE.ticker.BTC-USDT` | only `coinbase.*` is supported today |
| `coinbase.trades.BTC-USD` | the channel is `matches`, not `trades` |
| `coinbase.level2.BTC-USD` | the channel is `level2_batch` |

**Action:** fix the format. Don't retry the bad name.

---

## 8. Unsupported channel

**You did:** subscribed to a topic with a channel the hub doesn't ingest
(e.g. `coinbase.heartbeat.BTC-USD`, `coinbase.full.BTC-USD`).

**You see:** the topic validates *syntactically* (3 parts, correct
character classes), so the WS server accepts the subscribe and registers
you. But ingest filters out unsupported channels — see hub log
`ingest.skip_unsupported_channel`. No frames will ever arrive.

**Action:** use one of the supported channels: `ticker`, `matches`,
`level2_batch`. List in [topics.md](topics.md).

---

## 9. Connection refused / hub not running

**You did:** tried to connect WS to `ws://localhost:8765` or HTTP to
`http://localhost:8080/status` and got a connection-refused error.

**You see:** TCP-level connect failure (ECONNREFUSED on Unix, or `[Errno 61]`
on macOS).

**Action:**
- Confirm the hub process is running.
- Confirm you're hitting the right port (8765 for WS, 8080 for HTTP — they
  are *not* interchangeable).
- Confirm you're hitting the right host. The hub binds to `0.0.0.0` by
  default; `localhost` should work locally but not from another container.
- In Docker: ensure the port is published (`-p 8765:8765 -p 8080:8080`) and
  the container is healthy (`docker ps`).

---

## When in doubt: read `/status`

Almost every "is the hub OK?" question is answered by a single
`GET /status` call. The shape includes:

- `hub_uptime_seconds` — has the hub been up long enough to be useful?
- `upstreams[].state` — is Coinbase connected? reconnecting? disconnected?
- `upstreams[].reconnect_count` — has it been flapping?
- `consumers[].dropped_messages` — is anyone behind?
- `topics[].messages_per_second` — are we actually getting data?

If `/status` works but a specific topic isn't behaving, the failure is at
the topic level (cases 1, 2, 7, 8). If `/status` itself is unreachable or
shows reconnect loops, the failure is at the hub or upstream level (cases
3, 4, 5, 9).
