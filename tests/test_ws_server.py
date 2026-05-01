"""Downstream WebSocket server — end-to-end tests.

We bind a real ``websockets`` server to an ephemeral port and connect a real
``websockets`` client. That gives us authentic protocol behaviour (close codes,
back-pressure, async-iter teardown) without inventing a parallel set of fakes.

Plan-named verifies (Step 7):
    - subscribe / unsubscribe protocol
    - registry integration (ref-count fires on first/last)
    - clean tear-down on disconnect (no leaked subscriptions)

Plus the supporting properties: malformed-frame tolerance, multi-consumer
fan-out, idempotent subscribe, server-side stop closes active connections,
and the back-pressure cap.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
from datetime import UTC, datetime
from decimal import Decimal

import pytest
from websockets.asyncio.client import connect
from websockets.exceptions import ConnectionClosed

from market_data_broker.bus import InMemoryBus
from market_data_broker.models import Message, Ticker
from market_data_broker.registry import Registry
from market_data_broker.server.ws import DownstreamWSServer

TOPIC_BTC = "coinbase.ticker.BTC-USD"
TOPIC_ETH = "coinbase.ticker.ETH-USD"


# ---------------------------------------------------------------------------
# Fixtures / helpers
# ---------------------------------------------------------------------------


def _ticker_msg(topic: str = TOPIC_BTC, *, price: str = "50000.00") -> Message:
    payload = Ticker(
        type="ticker",
        product_id=topic.split(".")[-1],
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
    )
    return Message(
        topic=topic,
        venue="coinbase",
        received_at=datetime(2026, 5, 1, 12, 0, 0, tzinfo=UTC),
        payload=payload,
    )


class _CallbackRecorder:
    def __init__(self) -> None:
        self.calls: list[str] = []

    async def __call__(self, topic: str) -> None:
        self.calls.append(topic)


class _Stack:
    """Bus + registry + server, all bound to an ephemeral port."""

    def __init__(
        self,
        bus: InMemoryBus,
        registry: Registry,
        server: DownstreamWSServer,
        on_first: _CallbackRecorder,
        on_last: _CallbackRecorder,
    ) -> None:
        self.bus = bus
        self.registry = registry
        self.server = server
        self.on_first = on_first
        self.on_last = on_last

    @property
    def url(self) -> str:
        return f"ws://127.0.0.1:{self.server.port}"


async def _make_stack(
    *,
    max_dropped_messages: int | None = 100,
    max_queue: int | None = None,
) -> _Stack:
    bus = InMemoryBus()
    on_first = _CallbackRecorder()
    on_last = _CallbackRecorder()
    registry = Registry(bus, on_first_subscriber=on_first, on_last_unsubscriber=on_last)
    server = DownstreamWSServer(
        registry,
        host="127.0.0.1",
        port=0,
        max_dropped_messages=max_dropped_messages,
        max_queue=max_queue,
    )
    await server.start()
    return _Stack(bus, registry, server, on_first, on_last)


async def _wait_for(predicate, *, timeout: float = 2.0, interval: float = 0.01) -> None:
    deadline = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < deadline:
        if predicate():
            return
        await asyncio.sleep(interval)
    raise AssertionError(f"predicate did not hold within {timeout}s")


async def _read_until(
    ws,
    *,
    kind: str | None = None,
    pred=None,
    timeout: float = 2.0,
) -> dict:
    """Drain frames from the connection until one matches.

    Either ``kind`` or ``pred`` must be supplied. Useful because the server
    sends a ``welcome`` frame on connect; tests usually want the frame *after*
    that (or a specific ack/data/error)."""
    assert kind is not None or pred is not None
    if pred is None:
        def pred(frame: dict) -> bool:  # type: ignore[misc]
            return frame.get("kind") == kind
    deadline = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < deadline:
        remaining = deadline - asyncio.get_running_loop().time()
        raw = await asyncio.wait_for(ws.recv(), timeout=remaining)
        frame = json.loads(raw)
        if pred(frame):
            return frame
    raise AssertionError(f"no matching frame within {timeout}s")


async def _connect(stack: _Stack):
    """Open a client and consume the ``welcome`` frame so callers start clean."""
    ws = await connect(stack.url, max_size=2 * 1024 * 1024)
    welcome = json.loads(await asyncio.wait_for(ws.recv(), timeout=2.0))
    assert welcome["kind"] == "welcome"
    assert welcome["consumer_id"].startswith("ws-")
    return ws, welcome["consumer_id"]


# ---------------------------------------------------------------------------
# Lifecycle
# ---------------------------------------------------------------------------


async def test_start_is_idempotent() -> None:
    stack = await _make_stack()
    try:
        port = stack.server.port
        await stack.server.start()  # should not rebind
        assert stack.server.port == port
    finally:
        await stack.server.stop()


async def test_stop_is_idempotent() -> None:
    stack = await _make_stack()
    await stack.server.stop()
    await stack.server.stop()  # second call must not raise


async def test_port_property_reflects_actual_bound_port() -> None:
    stack = await _make_stack()
    try:
        assert stack.server.port > 0
    finally:
        await stack.server.stop()


# ---------------------------------------------------------------------------
# Welcome frame + smoke
# ---------------------------------------------------------------------------


async def test_welcome_frame_sent_on_connect() -> None:
    stack = await _make_stack()
    try:
        async with connect(stack.url) as ws:
            raw = await asyncio.wait_for(ws.recv(), timeout=2.0)
            frame = json.loads(raw)
            assert frame["kind"] == "welcome"
            assert frame["consumer_id"].startswith("ws-")
    finally:
        await stack.server.stop()


async def test_subscribe_and_receive_message() -> None:
    """End-to-end: client subscribes, hub publishes, client receives."""
    stack = await _make_stack()
    try:
        async with connect(stack.url) as ws:
            await ws.recv()  # welcome
            await ws.send(json.dumps({"action": "subscribe", "topics": [TOPIC_BTC]}))
            ack = await _read_until(ws, kind="ack")
            assert ack == {"kind": "ack", "action": "subscribe", "topics": [TOPIC_BTC]}

            # Once the ack is back, the registry add_topic has completed →
            # the bus subscription has the topic. Safe to publish now.
            await stack.bus.publish(_ticker_msg(price="51000.00"))

            data = await _read_until(ws, kind="data")
            assert data["kind"] == "data"
            assert data["topic"] == TOPIC_BTC
            assert data["venue"] == "coinbase"
            assert data["payload"]["type"] == "ticker"
            assert data["payload"]["price"] == "51000.00"  # Decimal preserved as str
            assert data["payload"]["product_id"] == "BTC-USD"
    finally:
        await stack.server.stop()


# ---------------------------------------------------------------------------
# Registry integration — the headline plan-verify
# ---------------------------------------------------------------------------


async def test_first_subscriber_callback_fires_on_subscribe() -> None:
    stack = await _make_stack()
    try:
        async with connect(stack.url) as ws:
            await ws.recv()
            await ws.send(json.dumps({"action": "subscribe", "topics": [TOPIC_BTC]}))
            await _read_until(ws, kind="ack")
            assert stack.on_first.calls == [TOPIC_BTC]
            assert stack.on_last.calls == []
    finally:
        await stack.server.stop()


async def test_last_unsubscriber_callback_fires_on_explicit_unsubscribe() -> None:
    stack = await _make_stack()
    try:
        async with connect(stack.url) as ws:
            await ws.recv()
            await ws.send(json.dumps({"action": "subscribe", "topics": [TOPIC_BTC]}))
            await _read_until(ws, kind="ack")
            await ws.send(json.dumps({"action": "unsubscribe", "topics": [TOPIC_BTC]}))
            ack = await _read_until(ws, kind="ack")
            assert ack["action"] == "unsubscribe"
            assert ack["topics"] == [TOPIC_BTC]
            assert stack.on_last.calls == [TOPIC_BTC]
    finally:
        await stack.server.stop()


async def test_disconnect_releases_topics_no_leak() -> None:
    """The headline 'no leaks on disconnect' guarantee."""
    stack = await _make_stack()
    try:
        async with connect(stack.url) as ws:
            await ws.recv()
            await ws.send(
                json.dumps({"action": "subscribe", "topics": [TOPIC_BTC, TOPIC_ETH]})
            )
            await _read_until(ws, kind="ack")
            assert sorted(stack.on_first.calls) == sorted([TOPIC_BTC, TOPIC_ETH])
            assert stack.registry.is_topic_active(TOPIC_BTC)
            assert stack.registry.is_topic_active(TOPIC_ETH)
        # WS closed: handler finally must unregister consumer → both topics
        # transition 1 → 0 → on_last fires for both.
        await _wait_for(lambda: len(stack.on_last.calls) == 2)
        assert sorted(stack.on_last.calls) == sorted([TOPIC_BTC, TOPIC_ETH])
        assert not stack.registry.is_topic_active(TOPIC_BTC)
        assert not stack.registry.is_topic_active(TOPIC_ETH)
    finally:
        await stack.server.stop()


async def test_two_consumers_share_topic_only_first_fires_callback() -> None:
    stack = await _make_stack()
    try:
        async with connect(stack.url) as ws_a, connect(stack.url) as ws_b:
            await ws_a.recv()
            await ws_b.recv()
            await ws_a.send(json.dumps({"action": "subscribe", "topics": [TOPIC_BTC]}))
            await _read_until(ws_a, kind="ack")
            await ws_b.send(json.dumps({"action": "subscribe", "topics": [TOPIC_BTC]}))
            await _read_until(ws_b, kind="ack")
            # First-subscriber fires once total even though two clients subbed.
            assert stack.on_first.calls == [TOPIC_BTC]
            assert stack.on_last.calls == []

            await stack.bus.publish(_ticker_msg())
            data_a = await _read_until(ws_a, kind="data")
            data_b = await _read_until(ws_b, kind="data")
            assert data_a["topic"] == TOPIC_BTC
            assert data_b["topic"] == TOPIC_BTC

        # Both clients gone → on_last fires exactly once.
        await _wait_for(lambda: stack.on_last.calls == [TOPIC_BTC])
    finally:
        await stack.server.stop()


async def test_each_connection_gets_unique_consumer_id() -> None:
    stack = await _make_stack()
    try:
        async with connect(stack.url) as ws_a, connect(stack.url) as ws_b:
            id_a = json.loads(await ws_a.recv())["consumer_id"]
            id_b = json.loads(await ws_b.recv())["consumer_id"]
            assert id_a != id_b
    finally:
        await stack.server.stop()


# ---------------------------------------------------------------------------
# Subscribe / unsubscribe semantics
# ---------------------------------------------------------------------------


async def test_idempotent_subscribe_does_not_double_fire() -> None:
    stack = await _make_stack()
    try:
        async with connect(stack.url) as ws:
            await ws.recv()
            await ws.send(json.dumps({"action": "subscribe", "topics": [TOPIC_BTC]}))
            await _read_until(ws, kind="ack")
            await ws.send(json.dumps({"action": "subscribe", "topics": [TOPIC_BTC]}))
            await _read_until(ws, kind="ack")
            assert stack.on_first.calls == [TOPIC_BTC]
    finally:
        await stack.server.stop()


async def test_unsubscribe_topic_not_held_is_no_op() -> None:
    stack = await _make_stack()
    try:
        async with connect(stack.url) as ws:
            await ws.recv()
            await ws.send(json.dumps({"action": "unsubscribe", "topics": [TOPIC_BTC]}))
            ack = await _read_until(ws, kind="ack")
            assert ack["action"] == "unsubscribe"
            assert stack.on_last.calls == []
    finally:
        await stack.server.stop()


async def test_unsubscribe_stops_message_flow() -> None:
    stack = await _make_stack()
    try:
        async with connect(stack.url) as ws:
            await ws.recv()
            await ws.send(json.dumps({"action": "subscribe", "topics": [TOPIC_BTC]}))
            await _read_until(ws, kind="ack")

            await stack.bus.publish(_ticker_msg(price="100.00"))
            data = await _read_until(ws, kind="data")
            assert data["payload"]["price"] == "100.00"

            await ws.send(json.dumps({"action": "unsubscribe", "topics": [TOPIC_BTC]}))
            await _read_until(ws, kind="ack")

            # After unsub: a fresh publish must NOT reach this client.
            await stack.bus.publish(_ticker_msg(price="999.00"))
            with pytest.raises(asyncio.TimeoutError):
                await asyncio.wait_for(ws.recv(), timeout=0.2)
    finally:
        await stack.server.stop()


async def test_partial_invalid_topic_in_batch_still_applies_valid_ones() -> None:
    stack = await _make_stack()
    try:
        async with connect(stack.url) as ws:
            await ws.recv()
            await ws.send(
                json.dumps(
                    {
                        "action": "subscribe",
                        "topics": [TOPIC_BTC, "not-a-valid-topic"],
                    }
                )
            )
            err = await _read_until(ws, kind="error")
            assert err["code"] == "invalid_topic"
            assert err["topic"] == "not-a-valid-topic"
            ack = await _read_until(ws, kind="ack")
            assert ack["topics"] == [TOPIC_BTC]
            assert stack.on_first.calls == [TOPIC_BTC]
    finally:
        await stack.server.stop()


# ---------------------------------------------------------------------------
# Malformed input — never tear down the session
# ---------------------------------------------------------------------------


async def test_bad_json_returns_error_keeps_session_alive() -> None:
    stack = await _make_stack()
    try:
        async with connect(stack.url) as ws:
            await ws.recv()
            await ws.send("{not valid json")
            err = await _read_until(ws, kind="error")
            assert err["code"] == "bad_json"
            # Session still alive: subscribe still works.
            await ws.send(json.dumps({"action": "subscribe", "topics": [TOPIC_BTC]}))
            ack = await _read_until(ws, kind="ack")
            assert ack["topics"] == [TOPIC_BTC]
    finally:
        await stack.server.stop()


async def test_non_object_frame_returns_error() -> None:
    stack = await _make_stack()
    try:
        async with connect(stack.url) as ws:
            await ws.recv()
            await ws.send(json.dumps([1, 2, 3]))
            err = await _read_until(ws, kind="error")
            assert err["code"] == "bad_request"
    finally:
        await stack.server.stop()


async def test_unknown_action_returns_error() -> None:
    stack = await _make_stack()
    try:
        async with connect(stack.url) as ws:
            await ws.recv()
            await ws.send(json.dumps({"action": "delete", "topics": [TOPIC_BTC]}))
            err = await _read_until(ws, kind="error")
            assert err["code"] == "bad_request"
            assert "delete" in err["message"]
    finally:
        await stack.server.stop()


async def test_topics_must_be_list_of_strings() -> None:
    stack = await _make_stack()
    try:
        async with connect(stack.url) as ws:
            await ws.recv()
            await ws.send(json.dumps({"action": "subscribe", "topics": "BTC"}))
            err = await _read_until(ws, kind="error")
            assert err["code"] == "bad_request"

            await ws.send(json.dumps({"action": "subscribe", "topics": [1, 2]}))
            err = await _read_until(ws, kind="error")
            assert err["code"] == "bad_request"
    finally:
        await stack.server.stop()


async def test_invalid_topic_returns_error_with_topic_field() -> None:
    stack = await _make_stack()
    try:
        async with connect(stack.url) as ws:
            await ws.recv()
            await ws.send(json.dumps({"action": "subscribe", "topics": ["bad..topic"]}))
            err = await _read_until(ws, kind="error")
            assert err["code"] == "invalid_topic"
            assert err["topic"] == "bad..topic"
    finally:
        await stack.server.stop()


# ---------------------------------------------------------------------------
# Server-initiated disconnects
# ---------------------------------------------------------------------------


async def test_server_stop_closes_active_connections() -> None:
    stack = await _make_stack()
    ws = await connect(stack.url)
    await ws.recv()  # welcome
    await ws.send(json.dumps({"action": "subscribe", "topics": [TOPIC_BTC]}))
    await _read_until(ws, kind="ack")

    await stack.server.stop()

    # The client side observes the close. Either recv raises ConnectionClosed,
    # or returns the close frame and a subsequent recv raises.
    with pytest.raises(ConnectionClosed):
        for _ in range(10):
            await asyncio.wait_for(ws.recv(), timeout=2.0)

    # Stop's wait_closed joined the handler tasks → registry is clean.
    assert not stack.registry.is_topic_active(TOPIC_BTC)
    assert stack.on_last.calls == [TOPIC_BTC]


async def test_backpressure_threshold_disconnects_slow_consumer() -> None:
    """Client never reads; server queue fills, drops add up, threshold hit."""
    # Tiny queue + tiny threshold so the test is fast and deterministic.
    stack = await _make_stack(max_queue=4, max_dropped_messages=5)
    try:
        ws = await connect(stack.url)
        await ws.recv()  # welcome
        await ws.send(json.dumps({"action": "subscribe", "topics": [TOPIC_BTC]}))
        await _read_until(ws, kind="ack")

        # Consume one data frame so we know the forward loop is running, then
        # stop reading for the rest of the test.
        await stack.bus.publish(_ticker_msg(price="1.0"))
        await _read_until(ws, kind="data")

        # Flood — far more than (max_queue + max_dropped_messages).
        for i in range(200):
            await stack.bus.publish(_ticker_msg(price=f"{i}.0"))

        # Pump the event loop; eventually the forward loop sees the threshold
        # crossed and closes the connection. Drain whatever is queued client-side
        # until the close lands.
        async def drain_until_closed() -> None:
            try:
                while True:
                    await asyncio.wait_for(ws.recv(), timeout=2.0)
            except ConnectionClosed:
                return

        await asyncio.wait_for(drain_until_closed(), timeout=3.0)
        assert ws.close_code == 1013
    finally:
        with contextlib.suppress(Exception):
            await ws.close()
        await stack.server.stop()


# ---------------------------------------------------------------------------
# Status / introspection
# ---------------------------------------------------------------------------


async def test_status_includes_ws_consumer() -> None:
    stack = await _make_stack()
    try:
        async with connect(stack.url) as ws:
            await ws.recv()
            await ws.send(json.dumps({"action": "subscribe", "topics": [TOPIC_BTC]}))
            await _read_until(ws, kind="ack")

            status = stack.registry.status()
            consumer_ids = [c["consumer_id"] for c in status["consumers"]]
            assert any(cid.startswith("ws-") for cid in consumer_ids)
            ws_consumer = next(c for c in status["consumers"] if c["consumer_id"].startswith("ws-"))
            assert TOPIC_BTC in ws_consumer["topics"]
    finally:
        await stack.server.stop()
