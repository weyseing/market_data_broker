"""MCP server — LLM-facing surface for the hub.

Same hub state (bus, registry, snapshot) as :mod:`server.ws` and
:mod:`server.status`; different transport. Built on FastMCP from the official
MCP Python SDK so we get JSON-Schema generation, both stdio and
streamable-http transports, and inspector compatibility for free.

Tools exposed
-------------

==========================  ============================================
Tool                        When the agent should call it
==========================  ============================================
``list_topics``             "What can I subscribe to?"
``describe_topic``          "What does a frame on this topic look like?"
``get_snapshot``            "What's the latest known state for SYMBOL?"
``get_hub_status``          "Is the hub healthy? Any reconnect loops?"
``stream_topic``            "Give me N live frames (poll-based, capped)."
==========================  ============================================

Tool descriptions are written for an LLM caller — they explain *when* to use
the tool, not just what it returns. The schemas come from Python type hints;
FastMCP generates JSON-Schema automatically.

Notes for the ``stream_topic`` tool
-----------------------------------

It registers a transient registry consumer named ``mcp-stream-N``. Going
through the registry is intentional: a poll on a topic that no one else holds
must fire ``on_first_subscriber`` so ingest sends an upstream subscribe. The
consumer is unregistered in a ``finally`` block, which drives ``on_last_unsubscriber``
exactly when expected — the same no-leaks guarantee as the WS server.
"""

from __future__ import annotations

import asyncio
import contextlib
import itertools
from typing import TYPE_CHECKING, Any

from mcp.server.fastmcp import FastMCP

from ..logging_config import get_logger
from ..topics import (
    COINBASE_CHANNELS_IN_SCOPE,
    VENUE_COINBASE,
    InvalidTopicError,
    parse_topic,
    topic_for,
)

if TYPE_CHECKING:
    from ..registry import Registry
    from ..snapshot import SnapshotStore


_stream_consumer_counter = itertools.count(1)


# Static catalogue of channel metadata used by ``describe_topic``. Examples are
# real frames captured during the Step-1.5 spike — see
# ``progress/20260425_spike_coinbase_findings.md``. Keeping them inline (rather
# than a separate JSON file) keeps the tool self-contained for stdio deployments
# where there is no repo to read from.
_CHANNEL_CATALOG: dict[str, dict[str, Any]] = {
    "ticker": {
        "summary": "One frame per trade; carries trade price + post-trade top-of-book.",
        "cadence": "One frame per trade (~5 Hz on BTC-USD during US hours).",
        "payload_type": "ticker",
        "fields": {
            "type": "literal 'ticker'",
            "product_id": "string, e.g. 'BTC-USD'",
            "sequence": "int (monotonic per product)",
            "trade_id": "int",
            "time": "ISO-8601 UTC string",
            "price": "string Decimal (trade price)",
            "last_size": "string Decimal",
            "side": "'buy' | 'sell' (taker side)",
            "best_bid": "string Decimal (post-trade)",
            "best_bid_size": "string Decimal",
            "best_ask": "string Decimal (post-trade)",
            "best_ask_size": "string Decimal",
            "open_24h": "string Decimal",
            "high_24h": "string Decimal",
            "low_24h": "string Decimal",
            "volume_24h": "string Decimal",
            "volume_30d": "string Decimal",
        },
        "example": {
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
            "volume_30d": "261517.66959213",
        },
        "notes": "Mid price = (best_bid + best_ask) / 2. Use Decimal arithmetic.",
    },
    "matches": {
        "summary": "One frame per executed trade. Lighter than ticker (no top-of-book).",
        "cadence": "One frame per trade.",
        "payload_type": "match | last_match",
        "fields": {
            "type": "'match' (live) or 'last_match' (catch-up on first frame after subscribe)",
            "product_id": "string",
            "sequence": "int",
            "trade_id": "int",
            "time": "ISO-8601 UTC string",
            "price": "string Decimal",
            "size": "string Decimal",
            "side": "'buy' | 'sell' (taker side)",
            "maker_order_id": "string UUID",
            "taker_order_id": "string UUID",
        },
        "example": {
            "type": "last_match",
            "product_id": "BTC-USD",
            "sequence": 127113513369,
            "trade_id": 1008094908,
            "time": "2026-04-25T13:53:49.062102Z",
            "price": "77706.48",
            "size": "0.0000001",
            "side": "buy",
            "maker_order_id": "13de9002-7b7f-4728-a51a-eb59ccbbcd43",
            "taker_order_id": "a68a6f59-4d2c-4b01-8656-4147e24e89cf",
        },
        "notes": "First frame after subscribe is 'last_match' (catch-up); rest are 'match'.",
    },
    "level2_batch": {
        "summary": "Initial top-50 book snapshot, then incremental updates.",
        "cadence": "ONE 'snapshot' on subscribe (~1 MB), then continuous 'l2update' frames.",
        "payload_type": "snapshot | l2update",
        "fields": {
            "snapshot.type": "literal 'snapshot'",
            "snapshot.product_id": "string",
            "snapshot.asks": "list of [price, size] pairs (strings)",
            "snapshot.bids": "list of [price, size] pairs (strings)",
            "l2update.type": "literal 'l2update'",
            "l2update.product_id": "string",
            "l2update.time": "ISO-8601 UTC string",
            "l2update.changes": (
                "list of [side, price, size] triples (strings); size '0' removes the level"
            ),
        },
        "example": {
            "type": "l2update",
            "product_id": "BTC-USD",
            "time": "2026-04-25T13:54:01.123456Z",
            "changes": [
                ["buy", "77697.01", "0.13838760"],
                ["buy", "77647.43", "0.00000000"],
            ],
        },
        "notes": (
            "Coinbase aliases level2_batch -> level2_50 (top-50 only, not full depth). "
            "The hub does not reconstruct the book from updates; consumers that need "
            "full depth maintain it themselves from the snapshot + updates."
        ),
    },
}


_INSTRUCTIONS = """\
Real-time crypto market-data hub (Coinbase). Exposes live ticks, executed
trades, and top-of-book L2 snapshots.

Use these tools when an agent needs CURRENT state — last trade, best bid/ask,
recent ticks. The hub does NOT store history: there is no "BTC price one hour
ago". Anything outside Coinbase, single-venue, or the three supported channels
will return null/empty/error rather than guess.

Numerics are JSON strings end-to-end (Decimal precision). Convert to Decimal
before doing arithmetic — never to float.
""".strip()


class MarketDataMCPServer:
    """FastMCP wrapper exposing the hub over MCP.

    Construction is decoupled from transport: build the server with a
    registry + snapshot, then call :meth:`run_stdio` or :meth:`run_http`
    depending on deployment. The same instance can also be exercised tool-by-tool
    in tests via the :meth:`tool_*` methods.
    """

    DEFAULT_STREAM_MAX_MESSAGES = 100
    DEFAULT_STREAM_TIMEOUT_MS = 5000
    # Hard caps so a buggy caller can't ask for unbounded work.
    HARD_MAX_MESSAGES = 5000
    HARD_TIMEOUT_MS = 60_000

    def __init__(
        self,
        *,
        registry: Registry,
        snapshot: SnapshotStore,
        host: str = "127.0.0.1",
        port: int = 8000,
    ) -> None:
        self._registry = registry
        self._snapshot = snapshot
        self._log = get_logger("server.mcp")
        self._app = FastMCP(
            name="market-data-broker",
            instructions=_INSTRUCTIONS,
            host=host,
            port=port,
        )
        self._register_tools()

    @property
    def app(self) -> FastMCP:
        """Underlying FastMCP instance (handy for tests + transport hooks)."""
        return self._app

    # ------------------------------------------------------------------
    # Transports
    # ------------------------------------------------------------------

    async def run_stdio(self) -> None:
        """Speak MCP over stdin/stdout — the Claude Desktop / inspector path."""
        await self._app.run_stdio_async()

    async def run_http(self) -> None:
        """Speak MCP over streamable-HTTP — the remote-agent / Docker path."""
        await self._app.run_streamable_http_async()

    # ------------------------------------------------------------------
    # Tool implementations (also callable directly for unit tests)
    # ------------------------------------------------------------------

    def tool_list_topics(self) -> dict[str, Any]:
        active = self._registry.active_topics()
        upstream_topics: set[str] = set()
        for entry in self._registry.status()["upstreams"]:
            upstream_topics.update(entry.get("topics", []))
        return {
            "venues": [VENUE_COINBASE],
            "channels": sorted(COINBASE_CHANNELS_IN_SCOPE),
            "topic_format": "{venue}.{channel}.{product_id}",
            "examples": [
                topic_for(VENUE_COINBASE, "ticker", "BTC-USD"),
                topic_for(VENUE_COINBASE, "matches", "ETH-USD"),
                topic_for(VENUE_COINBASE, "level2_batch", "SOL-USD"),
            ],
            "active_topics": sorted(active),
            "upstream_active_topics": sorted(upstream_topics),
            "note": (
                "The hub is demand-driven: a topic only exists in 'active_topics' "
                "while a downstream consumer wants it. Any valid "
                "{venue}.{channel}.{product_id} string is subscribable, even if "
                "not currently active."
            ),
        }

    def tool_describe_topic(self, topic: str) -> dict[str, Any]:
        try:
            parts = parse_topic(topic)
        except InvalidTopicError as exc:
            return {"error": "invalid_topic", "topic": topic, "message": str(exc)}
        if parts.venue != VENUE_COINBASE:
            return {
                "error": "unsupported_venue",
                "topic": topic,
                "message": f"only venue '{VENUE_COINBASE}' is supported today",
            }
        meta = _CHANNEL_CATALOG.get(parts.channel)
        if meta is None:
            return {
                "error": "unsupported_channel",
                "topic": topic,
                "message": (
                    f"channel {parts.channel!r} is not ingested; "
                    f"supported: {sorted(COINBASE_CHANNELS_IN_SCOPE)}"
                ),
            }
        return {
            "topic": topic,
            "venue": parts.venue,
            "channel": parts.channel,
            "product_id": parts.product_id,
            **meta,
        }

    def tool_get_snapshot(self, product_id: str) -> dict[str, Any]:
        snap = self._snapshot.get(product_id)
        if snap is None:
            return {
                "product_id": product_id,
                "found": False,
                "message": (
                    "No snapshot cached for this product. Either no consumer has "
                    "subscribed yet, or the product is unknown to Coinbase. Call "
                    "stream_topic first to warm the cache."
                ),
            }
        return {"product_id": product_id, "found": True, "snapshot": snap.to_dict()}

    def tool_get_hub_status(self) -> dict[str, Any]:
        return self._registry.status()

    async def tool_stream_topic(
        self,
        topic: str,
        max_messages: int = DEFAULT_STREAM_MAX_MESSAGES,
        timeout_ms: int = DEFAULT_STREAM_TIMEOUT_MS,
    ) -> dict[str, Any]:
        try:
            parse_topic(topic)
        except InvalidTopicError as exc:
            return {
                "topic": topic,
                "error": "invalid_topic",
                "message": str(exc),
                "messages": [],
                "received": 0,
            }
        if max_messages <= 0:
            return {
                "topic": topic,
                "error": "bad_request",
                "message": "max_messages must be > 0",
                "messages": [],
                "received": 0,
            }
        if timeout_ms <= 0:
            return {
                "topic": topic,
                "error": "bad_request",
                "message": "timeout_ms must be > 0",
                "messages": [],
                "received": 0,
            }
        cap_messages = min(max_messages, self.HARD_MAX_MESSAGES)
        cap_timeout = min(timeout_ms, self.HARD_TIMEOUT_MS) / 1000.0

        consumer_id = f"mcp-stream-{next(_stream_consumer_counter)}"
        sub = await self._registry.register_consumer(consumer_id, [topic])
        collected: list[dict[str, Any]] = []
        deadline = asyncio.get_running_loop().time() + cap_timeout
        try:
            while len(collected) < cap_messages:
                remaining = deadline - asyncio.get_running_loop().time()
                if remaining <= 0:
                    break
                try:
                    msg = await asyncio.wait_for(sub.get(), timeout=remaining)
                except TimeoutError:
                    break
                collected.append(msg.model_dump(mode="json"))
        finally:
            with contextlib.suppress(Exception):
                await self._registry.unregister_consumer(consumer_id)
        return {
            "topic": topic,
            "received": len(collected),
            "max_messages": cap_messages,
            "timeout_ms": int(cap_timeout * 1000),
            "messages": collected,
            "truncated_max_messages": max_messages > self.HARD_MAX_MESSAGES,
            "truncated_timeout_ms": timeout_ms > self.HARD_TIMEOUT_MS,
        }

    # ------------------------------------------------------------------
    # FastMCP registration
    # ------------------------------------------------------------------

    def _register_tools(self) -> None:
        # Closures so FastMCP sees zero-arg / typed signatures rather than
        # `self`. Descriptions are LLM-facing — explain WHEN to call.

        @self._app.tool(
            name="list_topics",
            description=(
                "List the topic taxonomy of the hub: every venue and channel the "
                "hub knows how to ingest, plus the topics a downstream consumer "
                "is currently holding. Call this first if you don't know what's "
                "subscribable. Returns the topic format, examples, and the set "
                "of active topics. Any valid {venue}.{channel}.{product_id} is "
                "subscribable even if not currently active — the hub is "
                "demand-driven."
            ),
        )
        def list_topics() -> dict[str, Any]:
            return self.tool_list_topics()

        @self._app.tool(
            name="describe_topic",
            description=(
                "Return the schema, update cadence, and a real example payload "
                "for a topic. Call this when you need to know what a frame on "
                "the topic looks like before subscribing or after seeing one. "
                "Topic format: {venue}.{channel}.{product_id} "
                "(e.g. 'coinbase.ticker.BTC-USD'). Returns an 'error' key if "
                "the topic format is invalid or the channel/venue is unsupported."
            ),
        )
        def describe_topic(topic: str) -> dict[str, Any]:
            return self.tool_describe_topic(topic)

        @self._app.tool(
            name="get_snapshot",
            description=(
                "Return the LAST KNOWN state for a product (last ticker, last "
                "trade, best bid/ask). Use when the user asks 'what is X right "
                "now' and you don't need a stream. Returns 'found': false if "
                "the cache is empty (no one has subscribed yet, or the product "
                "is unknown). Always includes 'last_update_at' so you can judge "
                "staleness. Numerics are strings — convert to Decimal before "
                "arithmetic."
            ),
        )
        def get_snapshot(product_id: str) -> dict[str, Any]:
            return self.tool_get_snapshot(product_id)

        @self._app.tool(
            name="get_hub_status",
            description=(
                "Return the hub's operational state: uptime, upstream connection "
                "state (connected/reconnecting/disconnected), reconnect counts, "
                "consumer list, dropped-message counts per consumer, and "
                "messages-per-second per topic. Call this when something looks "
                "wrong — stale data, no frames arriving, or to verify health "
                "before reporting a value to the user."
            ),
        )
        def get_hub_status() -> dict[str, Any]:
            return self.tool_get_hub_status()

        @self._app.tool(
            name="stream_topic",
            description=(
                "Drain up to 'max_messages' frames from a topic, or stop after "
                "'timeout_ms' milliseconds — whichever comes first. Poll-based: "
                "the hub does not push, the call returns when one of the two "
                "limits is hit. Use to sample live activity (e.g. confirm a "
                "topic is flowing) or to warm the snapshot cache for a topic "
                "that's been idle. For 'what's the latest' lookups prefer "
                "get_snapshot — it returns immediately. Hard caps: "
                "max_messages <= 5000, timeout_ms <= 60000."
            ),
        )
        async def stream_topic(
            topic: str,
            max_messages: int = self.DEFAULT_STREAM_MAX_MESSAGES,
            timeout_ms: int = self.DEFAULT_STREAM_TIMEOUT_MS,
        ) -> dict[str, Any]:
            return await self.tool_stream_topic(
                topic, max_messages=max_messages, timeout_ms=timeout_ms
            )
