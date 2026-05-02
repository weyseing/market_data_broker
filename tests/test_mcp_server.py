"""MCP server tests.

We test at two layers:

1. **Tool methods directly** — the ``tool_*`` methods on
   :class:`MarketDataMCPServer` are real Python callables. Calling them
   bypasses the FastMCP transport but exercises every piece of business
   logic we own. Fast and deterministic.

2. **FastMCP tool discovery** — confirm the five tools are registered with
   the names + descriptions Step-8 plan requires, and that the schemas
   render. This is what an MCP inspector / Claude Desktop will see.

Transport-level (stdio / streamable-http) verification is done manually with
the MCP inspector — see ``progress/20260425_implementation_plan.txt`` END-TO-END
VERIFY step 4.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from decimal import Decimal

import pytest

from market_data_broker.bus import InMemoryBus
from market_data_broker.models import Message, Ticker, Trade
from market_data_broker.registry import Registry
from market_data_broker.server.mcp_server import MarketDataMCPServer
from market_data_broker.snapshot import SnapshotStore

TOPIC_BTC = "coinbase.ticker.BTC-USD"
TOPIC_ETH = "coinbase.ticker.ETH-USD"


def _ticker(price: str = "50000.00", product_id: str = "BTC-USD") -> Message:
    return Message(
        topic=f"coinbase.ticker.{product_id}",
        venue="coinbase",
        received_at=datetime(2026, 5, 1, 12, 0, 0, tzinfo=UTC),
        payload=Ticker(
            type="ticker",
            product_id=product_id,
            sequence=1,
            trade_id=1,
            time=datetime(2026, 5, 1, 12, 0, 0, tzinfo=UTC),
            price=Decimal(price),
            last_size=Decimal("0.01"),
            side="buy",
            best_bid=Decimal("49999.99"),
            best_bid_size=Decimal("1.0"),
            best_ask=Decimal("50000.01"),
            best_ask_size=Decimal("1.0"),
            open_24h=Decimal("48000"),
            high_24h=Decimal("51000"),
            low_24h=Decimal("47000"),
            volume_24h=Decimal("1000"),
            volume_30d=Decimal("30000"),
        ),
    )


def _trade(product_id: str = "BTC-USD") -> Message:
    return Message(
        topic=f"coinbase.matches.{product_id}",
        venue="coinbase",
        received_at=datetime(2026, 5, 1, 12, 0, 0, tzinfo=UTC),
        payload=Trade(
            type="match",
            product_id=product_id,
            sequence=2,
            trade_id=2,
            time=datetime(2026, 5, 1, 12, 0, 0, tzinfo=UTC),
            price=Decimal("50000"),
            size=Decimal("0.5"),
            side="sell",
            maker_order_id="m",
            taker_order_id="t",
        ),
    )


class _CallbackRecorder:
    def __init__(self) -> None:
        self.calls: list[str] = []

    async def __call__(self, topic: str) -> None:
        self.calls.append(topic)


async def _make_server(*, with_snapshot: bool = True) -> tuple[
    MarketDataMCPServer, InMemoryBus, Registry, SnapshotStore, _CallbackRecorder, _CallbackRecorder
]:
    bus = InMemoryBus()
    on_first = _CallbackRecorder()
    on_last = _CallbackRecorder()
    snapshot = SnapshotStore(bus)
    if with_snapshot:
        await snapshot.start()

    async def chained_first(topic: str) -> None:
        await on_first(topic)
        await snapshot.track(topic)

    async def chained_last(topic: str) -> None:
        await on_last(topic)
        await snapshot.untrack(topic)

    registry = Registry(
        bus,
        on_first_subscriber=chained_first,
        on_last_unsubscriber=chained_last,
    )
    server = MarketDataMCPServer(registry=registry, snapshot=snapshot)
    return server, bus, registry, snapshot, on_first, on_last


# ---------------------------------------------------------------------------
# list_topics
# ---------------------------------------------------------------------------


async def test_list_topics_reports_static_taxonomy() -> None:
    server, _, _, snapshot, _, _ = await _make_server()
    try:
        result = server.tool_list_topics()
        assert result["venues"] == ["coinbase"]
        assert sorted(result["channels"]) == ["level2_batch", "matches", "ticker"]
        assert "{venue}" in result["topic_format"]
        assert TOPIC_BTC in result["examples"]
        # Nothing subscribed → both lists empty.
        assert result["active_topics"] == []
        assert result["upstream_active_topics"] == []
    finally:
        await snapshot.stop()


async def test_list_topics_reflects_active_subscriptions() -> None:
    server, _, registry, snapshot, _, _ = await _make_server()
    try:
        await registry.register_consumer("c1", [TOPIC_BTC])
        result = server.tool_list_topics()
        assert TOPIC_BTC in result["active_topics"]
    finally:
        await snapshot.stop()


# ---------------------------------------------------------------------------
# describe_topic
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("channel", ["ticker", "matches", "level2_batch"])
async def test_describe_topic_returns_schema_and_example(channel: str) -> None:
    server, _, _, snapshot, _, _ = await _make_server()
    try:
        topic = f"coinbase.{channel}.BTC-USD"
        result = server.tool_describe_topic(topic)
        assert result["topic"] == topic
        assert result["channel"] == channel
        assert result["product_id"] == "BTC-USD"
        assert "summary" in result and "cadence" in result
        assert "fields" in result and isinstance(result["fields"], dict)
        assert "example" in result and isinstance(result["example"], dict)
    finally:
        await snapshot.stop()


async def test_describe_topic_rejects_invalid_format() -> None:
    server, _, _, snapshot, _, _ = await _make_server()
    try:
        result = server.tool_describe_topic("garbage")
        assert result["error"] == "invalid_topic"
        assert result["topic"] == "garbage"
    finally:
        await snapshot.stop()


async def test_describe_topic_rejects_unsupported_channel() -> None:
    server, _, _, snapshot, _, _ = await _make_server()
    try:
        result = server.tool_describe_topic("coinbase.full.BTC-USD")
        assert result["error"] == "unsupported_channel"
        assert "channel" in result["message"]
    finally:
        await snapshot.stop()


async def test_describe_topic_rejects_other_venue() -> None:
    server, _, _, snapshot, _, _ = await _make_server()
    try:
        result = server.tool_describe_topic("binance.ticker.BTC-USDT")
        assert result["error"] == "unsupported_venue"
    finally:
        await snapshot.stop()


# ---------------------------------------------------------------------------
# get_snapshot
# ---------------------------------------------------------------------------


async def test_get_snapshot_returns_not_found_when_empty() -> None:
    server, _, _, snapshot, _, _ = await _make_server()
    try:
        result = server.tool_get_snapshot("BTC-USD")
        assert result["found"] is False
        msg = result["message"].lower()
        assert "no snapshot" in msg or "no consumer" in msg
    finally:
        await snapshot.stop()


async def test_get_snapshot_returns_cached_state() -> None:
    server, bus, registry, snapshot, _, _ = await _make_server()
    try:
        # Drive a consumer so snapshot starts tracking the topic.
        await registry.register_consumer("c1", [TOPIC_BTC])
        await bus.publish(_ticker(price="51234.56"))
        # Drain any backlog so the snapshot store has applied the message.
        for _ in range(50):
            if (s := snapshot.get("BTC-USD")) and s.last_ticker is not None:
                break
            await asyncio.sleep(0.01)

        result = server.tool_get_snapshot("BTC-USD")
        assert result["found"] is True
        assert result["snapshot"]["product_id"] == "BTC-USD"
        assert result["snapshot"]["best_bid"] == "49999.99"
        assert result["snapshot"]["last_ticker"]["price"] == "51234.56"
    finally:
        await snapshot.stop()


# ---------------------------------------------------------------------------
# get_hub_status
# ---------------------------------------------------------------------------


async def test_get_hub_status_shape() -> None:
    server, _, _, snapshot, _, _ = await _make_server()
    try:
        status = server.tool_get_hub_status()
        for key in ("hub_started_at", "hub_uptime_seconds", "upstreams", "consumers", "topics"):
            assert key in status
        assert isinstance(status["upstreams"], list)
        assert isinstance(status["consumers"], list)
    finally:
        await snapshot.stop()


# ---------------------------------------------------------------------------
# stream_topic
# ---------------------------------------------------------------------------


async def test_stream_topic_drains_messages() -> None:
    server, bus, _, snapshot, _, _ = await _make_server()
    try:
        async def publisher() -> None:
            # Give the tool time to register before we publish.
            await asyncio.sleep(0.05)
            for i in range(3):
                await bus.publish(_ticker(price=f"{i}.00"))

        pub = asyncio.create_task(publisher())
        result = await server.tool_stream_topic(TOPIC_BTC, max_messages=3, timeout_ms=2000)
        await pub

        assert result["received"] == 3
        assert result["topic"] == TOPIC_BTC
        prices = [m["payload"]["price"] for m in result["messages"]]
        assert prices == ["0.00", "1.00", "2.00"]
    finally:
        await snapshot.stop()


async def test_stream_topic_respects_timeout() -> None:
    server, _, _, snapshot, _, _ = await _make_server()
    try:
        # Nobody is publishing → call must return after timeout_ms with []
        result = await server.tool_stream_topic(TOPIC_BTC, max_messages=10, timeout_ms=100)
        assert result["received"] == 0
        assert result["messages"] == []
    finally:
        await snapshot.stop()


async def test_stream_topic_fires_first_and_last_subscriber_callbacks() -> None:
    """The headline 'no leaks' guarantee — going through the registry means
    a stream_topic call drives ingest sub/unsub even for a topic nobody else
    holds. Verified by callback recorders since we don't run real ingest."""
    server, _, _, snapshot, on_first, on_last = await _make_server()
    try:
        await server.tool_stream_topic(TOPIC_BTC, max_messages=1, timeout_ms=50)
        assert on_first.calls == [TOPIC_BTC]
        assert on_last.calls == [TOPIC_BTC]
    finally:
        await snapshot.stop()


async def test_stream_topic_invalid_topic_returns_error_no_registration() -> None:
    server, _, registry, snapshot, on_first, _ = await _make_server()
    try:
        result = await server.tool_stream_topic("bad..topic", max_messages=1, timeout_ms=50)
        assert result["error"] == "invalid_topic"
        assert result["received"] == 0
        # Must NOT have called the callback or left a consumer behind.
        assert on_first.calls == []
        assert registry.consumer("mcp-stream-1") is None or registry.consumer(
            "mcp-stream-1"
        ).subscription.closed
    finally:
        await snapshot.stop()


async def test_stream_topic_clamps_to_hard_caps() -> None:
    """Caps echo back in the response. Use a short real timeout so the test
    doesn't actually wait HARD_TIMEOUT_MS — clamping is reflected via
    ``truncated_*`` flags either way."""
    server, _, _, snapshot, _, _ = await _make_server()
    try:
        # max_messages clamps; timeout stays small so the call returns quickly.
        result = await server.tool_stream_topic(
            TOPIC_BTC,
            max_messages=999_999,
            timeout_ms=100,
        )
        assert result["max_messages"] == MarketDataMCPServer.HARD_MAX_MESSAGES
        assert result["timeout_ms"] == 100
        assert result["truncated_max_messages"] is True
        assert result["truncated_timeout_ms"] is False
    finally:
        await snapshot.stop()


async def test_stream_topic_clamp_logic_for_timeout() -> None:
    """Verify the timeout clamp applies WITHOUT actually waiting the cap.
    We monkey-patch the class constant down to a tiny value so the test stays
    fast while still exercising the same code path."""
    server, _, _, snapshot, _, _ = await _make_server()
    try:
        original = MarketDataMCPServer.HARD_TIMEOUT_MS
        MarketDataMCPServer.HARD_TIMEOUT_MS = 50
        try:
            result = await server.tool_stream_topic(
                TOPIC_BTC,
                max_messages=1,
                timeout_ms=999_999,
            )
        finally:
            MarketDataMCPServer.HARD_TIMEOUT_MS = original
        assert result["timeout_ms"] == 50
        assert result["truncated_timeout_ms"] is True
    finally:
        await snapshot.stop()


async def test_stream_topic_rejects_zero_max_messages() -> None:
    server, _, _, snapshot, on_first, _ = await _make_server()
    try:
        result = await server.tool_stream_topic(TOPIC_BTC, max_messages=0, timeout_ms=100)
        assert result["error"] == "bad_request"
        assert on_first.calls == []
    finally:
        await snapshot.stop()


async def test_stream_topic_unregisters_on_exception() -> None:
    """If the inner await is cancelled, the consumer must still be cleaned up."""
    server, _, registry, snapshot, on_first, on_last = await _make_server()
    try:
        task = asyncio.create_task(
            server.tool_stream_topic(TOPIC_BTC, max_messages=10, timeout_ms=10_000)
        )
        # Let it register, then cancel.
        await asyncio.sleep(0.05)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        # Cancellation propagates through the finally block — registry is clean.
        assert on_first.calls == [TOPIC_BTC]
        assert on_last.calls == [TOPIC_BTC]
        assert not registry.is_topic_active(TOPIC_BTC)
    finally:
        await snapshot.stop()


# ---------------------------------------------------------------------------
# FastMCP discovery — confirm the five plan-named tools are registered with
# the right names and have non-trivial descriptions.
# ---------------------------------------------------------------------------


async def test_all_five_tools_registered_with_descriptions() -> None:
    server, _, _, snapshot, _, _ = await _make_server()
    try:
        tools = await server.app.list_tools()
        names = {t.name for t in tools}
        assert names == {
            "list_topics",
            "describe_topic",
            "get_snapshot",
            "get_hub_status",
            "stream_topic",
        }
        for t in tools:
            assert t.description and len(t.description) > 40, (
                f"tool {t.name} has missing/short description"
            )
    finally:
        await snapshot.stop()


async def test_tool_schemas_reflect_typed_arguments() -> None:
    server, _, _, snapshot, _, _ = await _make_server()
    try:
        tools = {t.name: t for t in await server.app.list_tools()}
        # describe_topic takes a single string ``topic``
        describe = tools["describe_topic"].inputSchema
        assert "topic" in describe.get("properties", {})
        assert describe["properties"]["topic"]["type"] == "string"
        # stream_topic has the right param set
        stream = tools["stream_topic"].inputSchema["properties"]
        assert set(stream.keys()) >= {"topic", "max_messages", "timeout_ms"}
    finally:
        await snapshot.stop()


async def test_call_tool_through_fastmcp_dispatches() -> None:
    """End-to-end invocation through the FastMCP dispatcher (not transport)."""
    server, _, _, snapshot, _, _ = await _make_server()
    try:
        result = await server.app.call_tool("describe_topic", {"topic": TOPIC_BTC})
        # call_tool returns (content, structured_content) in newer SDKs;
        # support both shapes.
        if isinstance(result, tuple):
            _, structured = result
        else:
            structured = result
        # FastMCP wraps the dict — unwrap if needed.
        if isinstance(structured, dict):
            payload = structured.get("result", structured)
        else:
            payload = structured
        assert isinstance(payload, dict)
        assert payload["topic"] == TOPIC_BTC
        assert payload["channel"] == "ticker"
    finally:
        await snapshot.stop()
