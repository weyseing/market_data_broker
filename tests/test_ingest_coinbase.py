"""Coinbase ingest adapter — fake-WS integration tests.

Plan-named verifies (Step 5):
    - clean connect + subscribe
    - reconnect with re-subscribe of the live set
    - stale heartbeat triggers reconnect

Plus the supporting properties: control-frame filtering, malformed-frame
tolerance, demand-driven (un)subscribe, batching by channel, registry
message-rate accounting, and clean stop.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import time
from typing import Any

import pytest

from market_data_broker.bus import InMemoryBus
from market_data_broker.ingest.coinbase import CoinbaseIngest
from market_data_broker.registry import Registry

TICKER_BTC = "coinbase.ticker.BTC-USD"
TICKER_ETH = "coinbase.ticker.ETH-USD"
MATCHES_BTC = "coinbase.matches.BTC-USD"
LEVEL2_BTC = "coinbase.level2_batch.BTC-USD"


# ---------------------------------------------------------------------------
# Test doubles
# ---------------------------------------------------------------------------

_END = object()  # sentinel pushed into FakeWS inbox to terminate the iterator


class FakeWS:
    """In-process stand-in for ``websockets.ClientConnection``.

    Tests push server-to-client frames via :meth:`push` and inspect what the
    ingest sent via :attr:`sent`. Closing the WS terminates the async iterator
    by pushing a sentinel.
    """

    def __init__(self) -> None:
        self.sent: list[dict[str, Any]] = []
        self._inbox: asyncio.Queue[Any] = asyncio.Queue()
        self.closed = False
        self.close_code: int | None = None
        self.close_reason: str | None = None

    async def send(self, data: str) -> None:
        if self.closed:
            raise ConnectionError("send on closed FakeWS")
        self.sent.append(json.loads(data))

    async def close(self, *, code: int = 1000, reason: str = "") -> None:
        if self.closed:
            return
        self.closed = True
        self.close_code = code
        self.close_reason = reason
        await self._inbox.put(_END)

    def push(self, frame: dict[str, Any] | str) -> None:
        """Server-to-client frame. Accepts dicts (json-encoded) or raw strings."""
        payload = frame if isinstance(frame, str) else json.dumps(frame)
        self._inbox.put_nowait(payload)

    def end(self) -> None:
        """Terminate the iterator without setting closed=True (simulates upstream
        disconnect from the server side)."""
        self._inbox.put_nowait(_END)

    def __aiter__(self) -> FakeWS:
        return self

    async def __anext__(self) -> str:
        item = await self._inbox.get()
        if item is _END:
            raise StopAsyncIteration
        return item


class FakeConnector:
    """Hands out FakeWS sessions in order. New session per call → tests can
    drive reconnect behaviour by inspecting :attr:`opened`.
    """

    def __init__(self) -> None:
        self._queue: list[FakeWS] = []
        self.opened: list[FakeWS] = []

    def add_session(self, ws: FakeWS | None = None) -> FakeWS:
        ws = ws or FakeWS()
        self._queue.append(ws)
        return ws

    def __call__(self) -> _FakeCM:
        if self._queue:
            ws = self._queue.pop(0)
        else:
            # Auto-create so tests don't have to pre-queue every reconnect.
            ws = FakeWS()
        self.opened.append(ws)
        return _FakeCM(ws)


class _FakeCM:
    def __init__(self, ws: FakeWS) -> None:
        self.ws = ws

    async def __aenter__(self) -> FakeWS:
        return self.ws

    async def __aexit__(self, *exc: object) -> None:
        if not self.ws.closed:
            await self.ws.close()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


async def _wait_for(predicate, *, timeout: float = 1.0, interval: float = 0.01) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        await asyncio.sleep(interval)
    raise AssertionError(f"predicate did not hold within {timeout}s")


def _make_hub(
    *,
    connector: FakeConnector | None = None,
    heartbeat_timeout_seconds: float = 60.0,
    initial_backoff_seconds: float = 0.01,
    max_backoff_seconds: float = 0.02,
) -> tuple[InMemoryBus, Registry, CoinbaseIngest, FakeConnector]:
    """Stand up bus + registry + ingest wired together. Returns all four so
    tests can drive each layer."""
    connector = connector or FakeConnector()
    bus = InMemoryBus()
    ingest = CoinbaseIngest(
        bus=bus,
        connector=connector,
        heartbeat_timeout_seconds=heartbeat_timeout_seconds,
        initial_backoff_seconds=initial_backoff_seconds,
        max_backoff_seconds=max_backoff_seconds,
    )
    registry = Registry(
        bus,
        on_first_subscriber=ingest.on_first_subscriber,
        on_last_unsubscriber=ingest.on_last_unsubscriber,
    )
    ingest.attach_registry(registry)
    return bus, registry, ingest, connector


def _ticker_frame(
    *,
    product_id: str = "BTC-USD",
    sequence: int = 1,
    trade_id: int = 1,
    price: str = "77668.92",
) -> dict[str, Any]:
    return {
        "type": "ticker",
        "product_id": product_id,
        "sequence": sequence,
        "trade_id": trade_id,
        "time": "2026-04-25T13:53:06.170361Z",
        "price": price,
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
    }


# ---------------------------------------------------------------------------
# Pure callback behaviour (no run loop needed)
# ---------------------------------------------------------------------------


async def test_first_subscriber_callback_populates_desired() -> None:
    _, _, ingest, _ = _make_hub()
    await ingest.on_first_subscriber(TICKER_BTC)
    assert ingest.desired_topics == frozenset({("ticker", "BTC-USD")})


async def test_last_unsubscriber_callback_removes_from_desired() -> None:
    _, _, ingest, _ = _make_hub()
    await ingest.on_first_subscriber(TICKER_BTC)
    await ingest.on_last_unsubscriber(TICKER_BTC)
    assert ingest.desired_topics == frozenset()


async def test_callback_ignores_non_coinbase_venue() -> None:
    _, _, ingest, _ = _make_hub()
    await ingest.on_first_subscriber("binance.ticker.BTC-USDT")
    assert ingest.desired_topics == frozenset()


async def test_callback_ignores_invalid_topic() -> None:
    _, _, ingest, _ = _make_hub()
    await ingest.on_first_subscriber("not-a-topic")
    assert ingest.desired_topics == frozenset()


async def test_callback_ignores_unsupported_channel() -> None:
    _, _, ingest, _ = _make_hub()
    # ``status`` is a real Coinbase channel but not in our supported set.
    await ingest.on_first_subscriber("coinbase.status.BTC-USD")
    assert ingest.desired_topics == frozenset()


# ---------------------------------------------------------------------------
# Clean connect + subscribe (plan-named verify)
# ---------------------------------------------------------------------------


async def test_subscribe_frame_sent_after_first_subscriber() -> None:
    _, registry, ingest, connector = _make_hub()
    await ingest.start()
    try:
        await registry.register_consumer("c1", [TICKER_BTC])
        await _wait_for(lambda: connector.opened and len(connector.opened[0].sent) >= 1)
        ws = connector.opened[0]
        assert ws.sent == [
            {
                "type": "subscribe",
                "product_ids": ["BTC-USD"],
                "channels": ["ticker"],
            }
        ]
    finally:
        await ingest.stop()


async def test_subscribe_batched_per_channel() -> None:
    """Two ticker products must go out as a single frame, not two."""
    _, registry, ingest, connector = _make_hub()
    await ingest.start()
    try:
        # Pre-populate desired BEFORE the connector hands out a session, so the
        # initial reconcile after connect handles them in one batch.
        await ingest.on_first_subscriber(TICKER_BTC)
        await ingest.on_first_subscriber(TICKER_ETH)
        await registry.register_consumer("c1", [TICKER_BTC, TICKER_ETH])
        await _wait_for(lambda: connector.opened and len(connector.opened[0].sent) >= 1)
        ws = connector.opened[0]
        # Find the ticker subscribe frame.
        ticker_frames = [f for f in ws.sent if f.get("channels") == ["ticker"]]
        assert len(ticker_frames) == 1
        assert sorted(ticker_frames[0]["product_ids"]) == ["BTC-USD", "ETH-USD"]
    finally:
        await ingest.stop()


async def test_subscribe_sends_one_frame_per_channel_when_mixing() -> None:
    _, registry, ingest, connector = _make_hub()
    await ingest.start()
    try:
        await ingest.on_first_subscriber(TICKER_BTC)
        await ingest.on_first_subscriber(MATCHES_BTC)
        await registry.register_consumer("c1", [TICKER_BTC, MATCHES_BTC])
        await _wait_for(lambda: connector.opened and len(connector.opened[0].sent) >= 2)
        ws = connector.opened[0]
        channels_sent = sorted(f["channels"][0] for f in ws.sent if f["type"] == "subscribe")
        assert channels_sent == ["matches", "ticker"]
    finally:
        await ingest.stop()


# ---------------------------------------------------------------------------
# Demand-driven unsubscribe
# ---------------------------------------------------------------------------


async def test_unsubscribe_frame_sent_after_last_unsubscriber() -> None:
    _, registry, ingest, connector = _make_hub()
    await ingest.start()
    try:
        await registry.register_consumer("c1", [TICKER_BTC])
        await _wait_for(lambda: connector.opened and len(connector.opened[0].sent) >= 1)
        ws = connector.opened[0]
        await registry.unregister_consumer("c1")
        await _wait_for(lambda: any(f["type"] == "unsubscribe" for f in ws.sent))
        unsub = next(f for f in ws.sent if f["type"] == "unsubscribe")
        assert unsub == {
            "type": "unsubscribe",
            "product_ids": ["BTC-USD"],
            "channels": ["ticker"],
        }
        assert ingest.desired_topics == frozenset()
    finally:
        await ingest.stop()


# ---------------------------------------------------------------------------
# Data frame plumbing
# ---------------------------------------------------------------------------


async def test_data_frame_published_to_bus() -> None:
    bus, registry, ingest, connector = _make_hub()
    await ingest.start()
    try:
        sub = await registry.register_consumer("c1", [TICKER_BTC])
        await _wait_for(lambda: connector.opened and len(connector.opened[0].sent) >= 1)
        ws = connector.opened[0]
        ws.push(_ticker_frame())
        msg = await asyncio.wait_for(sub.get(), timeout=1.0)
        assert msg.topic == TICKER_BTC
        assert msg.payload.type == "ticker"
        assert str(msg.payload.price) == "77668.92"
        # Registry's per-topic counter is incremented on publish.
        await _wait_for(lambda: registry.status()["topics"][0]["messages_total"] >= 1)
    finally:
        await ingest.stop()


async def test_control_frames_filtered() -> None:
    """subscriptions / heartbeat / error must not produce a Message and must
    not crash the receive loop."""
    bus, registry, ingest, connector = _make_hub()
    await ingest.start()
    try:
        sub = await registry.register_consumer("c1", [TICKER_BTC])
        await _wait_for(lambda: connector.opened and len(connector.opened[0].sent) >= 1)
        ws = connector.opened[0]

        # Push three control frames followed by one data frame; only the data
        # frame must reach the consumer.
        ws.push(
            {
                "type": "subscriptions",
                "channels": [{"name": "ticker", "product_ids": ["BTC-USD"]}],
            }
        )
        ws.push({"type": "heartbeat", "product_id": "BTC-USD"})
        ws.push({"type": "error", "message": "test error frame"})
        ws.push(_ticker_frame())

        msg = await asyncio.wait_for(sub.get(), timeout=1.0)
        assert msg.payload.type == "ticker"
        # Nothing else queued.
        assert sub.qsize() == 0
    finally:
        await ingest.stop()


async def test_invalid_json_does_not_crash_receive_loop() -> None:
    bus, registry, ingest, connector = _make_hub()
    await ingest.start()
    try:
        sub = await registry.register_consumer("c1", [TICKER_BTC])
        await _wait_for(lambda: connector.opened and len(connector.opened[0].sent) >= 1)
        ws = connector.opened[0]
        ws.push("not-json{{{")
        ws.push(_ticker_frame())
        msg = await asyncio.wait_for(sub.get(), timeout=1.0)
        assert msg.payload.type == "ticker"
    finally:
        await ingest.stop()


async def test_unknown_frame_type_does_not_crash() -> None:
    bus, registry, ingest, connector = _make_hub()
    await ingest.start()
    try:
        sub = await registry.register_consumer("c1", [TICKER_BTC])
        await _wait_for(lambda: connector.opened and len(connector.opened[0].sent) >= 1)
        ws = connector.opened[0]
        ws.push({"type": "wholly-new-channel-from-coinbase", "product_id": "BTC-USD"})
        ws.push(_ticker_frame())
        msg = await asyncio.wait_for(sub.get(), timeout=1.0)
        assert msg.payload.type == "ticker"
    finally:
        await ingest.stop()


async def test_validation_failure_does_not_crash() -> None:
    """Malformed ticker (missing required field) is logged and dropped, the
    next valid frame still gets through."""
    bus, registry, ingest, connector = _make_hub()
    await ingest.start()
    try:
        sub = await registry.register_consumer("c1", [TICKER_BTC])
        await _wait_for(lambda: connector.opened and len(connector.opened[0].sent) >= 1)
        ws = connector.opened[0]
        # missing price + most of the other fields
        ws.push({"type": "ticker", "product_id": "BTC-USD"})
        ws.push(_ticker_frame())
        msg = await asyncio.wait_for(sub.get(), timeout=1.0)
        assert msg.payload.type == "ticker"
    finally:
        await ingest.stop()


# ---------------------------------------------------------------------------
# Reconnect (plan-named verify)
# ---------------------------------------------------------------------------


async def test_reconnect_resubscribes_live_set() -> None:
    """When the WS dies mid-flight the ingest must reconnect and re-subscribe
    every topic in the live set on the new session."""
    _, registry, ingest, connector = _make_hub()
    await ingest.start()
    try:
        await registry.register_consumer("c1", [TICKER_BTC, MATCHES_BTC])
        await _wait_for(lambda: connector.opened and len(connector.opened[0].sent) >= 2)
        first_ws = connector.opened[0]
        # Server hangs up.
        first_ws.end()
        # Wait for a second session to open and receive subscribe frames.
        await _wait_for(lambda: len(connector.opened) >= 2, timeout=2.0)
        second_ws = connector.opened[1]
        await _wait_for(lambda: len(second_ws.sent) >= 2, timeout=2.0)
        channels_resent = sorted(
            f["channels"][0] for f in second_ws.sent if f["type"] == "subscribe"
        )
        assert channels_resent == ["matches", "ticker"]
    finally:
        await ingest.stop()


async def test_reconnect_after_send_failure() -> None:
    """If the reconcile loop's send() raises (closed socket), the run loop
    catches it and reconnects rather than crashing."""
    _, registry, ingest, connector = _make_hub()
    await ingest.start()
    try:
        await registry.register_consumer("c1", [TICKER_BTC])
        await _wait_for(lambda: connector.opened and len(connector.opened[0].sent) >= 1)
        first_ws = connector.opened[0]
        # Mark closed so any further send() (e.g. after a churn) errors.
        first_ws.closed = True
        first_ws.end()
        await _wait_for(lambda: len(connector.opened) >= 2, timeout=2.0)
    finally:
        await ingest.stop()


# ---------------------------------------------------------------------------
# Stale heartbeat (plan-named verify)
# ---------------------------------------------------------------------------


async def test_stale_feed_triggers_reconnect() -> None:
    _, registry, ingest, connector = _make_hub(heartbeat_timeout_seconds=0.1)
    await ingest.start()
    try:
        await registry.register_consumer("c1", [TICKER_BTC])
        await _wait_for(lambda: connector.opened and len(connector.opened[0].sent) >= 1)
        first_ws = connector.opened[0]
        # No frames pushed → watchdog should fire after ~0.1s.
        await _wait_for(lambda: first_ws.closed, timeout=2.0)
        assert first_ws.close_reason == "stale feed"
        # And ingest reconnects.
        await _wait_for(lambda: len(connector.opened) >= 2, timeout=2.0)
    finally:
        await ingest.stop()


async def test_watchdog_dormant_when_no_subscribers() -> None:
    """No desired topics → no expectation of frames → watchdog must not fire.

    This guards against a startup loop where an idle hub kills its own
    connection in a busy-loop."""
    _, _, ingest, connector = _make_hub(heartbeat_timeout_seconds=0.05)
    await ingest.start()
    try:
        await _wait_for(lambda: bool(connector.opened), timeout=1.0)
        first_ws = connector.opened[0]
        # Wait long enough that the watchdog WOULD have fired.
        await asyncio.sleep(0.2)
        assert not first_ws.closed
        assert len(connector.opened) == 1
    finally:
        await ingest.stop()


async def test_watchdog_resets_on_each_frame() -> None:
    """As long as frames keep arriving the watchdog must not fire."""
    bus, registry, ingest, connector = _make_hub(heartbeat_timeout_seconds=0.1)
    await ingest.start()
    try:
        sub = await registry.register_consumer("c1", [TICKER_BTC])
        await _wait_for(lambda: connector.opened and len(connector.opened[0].sent) >= 1)
        ws = connector.opened[0]
        # Push a frame every 30 ms for ~250 ms — well within the 100 ms window.
        for i in range(8):
            ws.push(_ticker_frame(sequence=100 + i, trade_id=100 + i))
            await asyncio.sleep(0.03)
        # Drain to free queue space.
        for _ in range(8):
            with contextlib.suppress(asyncio.TimeoutError):
                await asyncio.wait_for(sub.get(), timeout=0.05)
        assert not ws.closed
        assert len(connector.opened) == 1
    finally:
        await ingest.stop()


# ---------------------------------------------------------------------------
# Lifecycle & upstream state
# ---------------------------------------------------------------------------


async def test_upstream_state_transitions_through_lifecycle() -> None:
    _, registry, ingest, connector = _make_hub()
    await ingest.start()
    try:
        await _wait_for(
            lambda: registry.upstream("coinbase") is not None
            and registry.upstream("coinbase").state == "connected"
        )
    finally:
        await ingest.stop()
    upstream = registry.upstream("coinbase")
    assert upstream is not None
    assert upstream.state == "disconnected"


async def test_reconnect_increments_reconnect_count() -> None:
    _, registry, ingest, connector = _make_hub()
    await ingest.start()
    try:
        await _wait_for(
            lambda: registry.upstream("coinbase") is not None
            and registry.upstream("coinbase").state == "connected"
        )
        connector.opened[0].end()
        await _wait_for(
            lambda: registry.upstream("coinbase").reconnect_count >= 1, timeout=2.0
        )
    finally:
        await ingest.stop()


async def test_start_is_idempotent() -> None:
    _, _, ingest, _ = _make_hub()
    await ingest.start()
    try:
        first_runner = ingest._runner
        await ingest.start()  # should be a no-op
        assert ingest._runner is first_runner
    finally:
        await ingest.stop()


async def test_stop_is_idempotent() -> None:
    _, _, ingest, _ = _make_hub()
    await ingest.start()
    await ingest.stop()
    await ingest.stop()  # must not raise


async def test_attach_registry_required_before_start() -> None:
    bus = InMemoryBus()
    ingest = CoinbaseIngest(bus=bus, connector=FakeConnector())
    await ingest.start()
    try:
        # The runner task should fail because attach_registry was never called.
        await asyncio.sleep(0.05)
        assert ingest._runner is not None
        assert ingest._runner.done()
        with pytest.raises(RuntimeError, match="attach_registry"):
            ingest._runner.result()
    finally:
        await ingest.stop()


# ---------------------------------------------------------------------------
# No-leak sanity
# ---------------------------------------------------------------------------


async def test_no_leaked_subs_after_consumer_churn() -> None:
    """After a burst of register/unregister cycles, ``_desired`` and
    ``_active_upstream`` must be empty and the wire must be quiet on the
    current session."""
    _, registry, ingest, connector = _make_hub()
    await ingest.start()
    try:
        for i in range(10):
            cid = f"c{i}"
            await registry.register_consumer(cid, [TICKER_BTC, MATCHES_BTC])
            await registry.unregister_consumer(cid)
        # Let reconcile catch up.
        await _wait_for(
            lambda: not ingest.desired_topics and not ingest.active_upstream_topics,
            timeout=2.0,
        )
    finally:
        await ingest.stop()
