"""Tests for the snapshot store.

Plan verifies (Step 6): synthetic-stream tests pass.

We exercise:
- lifecycle (start / stop / restart / idempotency)
- payload-type dispatch (Ticker / Trade / L2Snapshot / L2Update)
- best bid/ask sourced from Ticker AND L2Snapshot (with defensive sort)
- L2Update refreshes timestamp without altering top-of-book
- multi-symbol isolation
- track / untrack semantics, including data retention after untrack
- the snapshot store does NOT affect registry refcounts (it's passive)
- to_dict round-trip preserves Decimal precision as strings
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from decimal import Decimal

import pytest

from market_data_broker.bus import InMemoryBus
from market_data_broker.models import (
    L2Change,
    L2Snapshot,
    L2Update,
    Message,
    Ticker,
    Trade,
)
from market_data_broker.registry import Registry
from market_data_broker.snapshot import SnapshotStore, SymbolSnapshot

TICKER_BTC = "coinbase.ticker.BTC-USD"
TICKER_ETH = "coinbase.ticker.ETH-USD"
MATCHES_BTC = "coinbase.matches.BTC-USD"
LEVEL2_BTC = "coinbase.level2_batch.BTC-USD"


# ---------------------------------------------------------------------------
# Message builders
# ---------------------------------------------------------------------------


def _ticker(
    product_id: str = "BTC-USD",
    *,
    price: str = "50000",
    best_bid: str = "49999",
    best_bid_size: str = "1.5",
    best_ask: str = "50001",
    best_ask_size: str = "2.0",
    received_at: datetime | None = None,
) -> Message:
    payload = Ticker(
        type="ticker",
        product_id=product_id,
        sequence=1,
        trade_id=1,
        time=datetime(2026, 5, 1, 12, 0, 0, tzinfo=UTC),
        price=Decimal(price),
        last_size=Decimal("0.01"),
        side="buy",
        best_bid=Decimal(best_bid),
        best_bid_size=Decimal(best_bid_size),
        best_ask=Decimal(best_ask),
        best_ask_size=Decimal(best_ask_size),
        open_24h=Decimal("48000"),
        high_24h=Decimal("51000"),
        low_24h=Decimal("47000"),
        volume_24h=Decimal("1000"),
        volume_30d=Decimal("30000"),
    )
    return Message(
        topic=f"coinbase.ticker.{product_id}",
        venue="coinbase",
        received_at=received_at or datetime(2026, 5, 1, 12, 0, 0, tzinfo=UTC),
        payload=payload,
    )


def _trade(
    product_id: str = "BTC-USD",
    *,
    type_: str = "match",
    price: str = "50000",
    size: str = "0.5",
    received_at: datetime | None = None,
) -> Message:
    payload = Trade(
        type=type_,  # type: ignore[arg-type]
        product_id=product_id,
        sequence=1,
        trade_id=1,
        time=datetime(2026, 5, 1, 12, 0, 0, tzinfo=UTC),
        price=Decimal(price),
        size=Decimal(size),
        side="buy",
        maker_order_id="m",
        taker_order_id="t",
    )
    return Message(
        topic=f"coinbase.matches.{product_id}",
        venue="coinbase",
        received_at=received_at or datetime(2026, 5, 1, 12, 0, 0, tzinfo=UTC),
        payload=payload,
    )


def _l2_snapshot(
    product_id: str = "BTC-USD",
    *,
    bids: list[tuple[str, str]] | None = None,
    asks: list[tuple[str, str]] | None = None,
    received_at: datetime | None = None,
) -> Message:
    payload = L2Snapshot(
        type="snapshot",
        product_id=product_id,
        bids=[(Decimal(p), Decimal(s)) for p, s in (bids or [("49000", "1.0")])],
        asks=[(Decimal(p), Decimal(s)) for p, s in (asks or [("51000", "1.0")])],
    )
    return Message(
        topic=f"coinbase.level2_batch.{product_id}",
        venue="coinbase",
        received_at=received_at or datetime(2026, 5, 1, 12, 0, 0, tzinfo=UTC),
        payload=payload,
    )


def _l2_update(
    product_id: str = "BTC-USD",
    *,
    received_at: datetime | None = None,
) -> Message:
    payload = L2Update(
        type="l2update",
        product_id=product_id,
        time=datetime(2026, 5, 1, 12, 0, 0, tzinfo=UTC),
        changes=[L2Change(side="buy", price=Decimal("49500"), size=Decimal("0.0"))],
    )
    return Message(
        topic=f"coinbase.level2_batch.{product_id}",
        venue="coinbase",
        received_at=received_at or datetime(2026, 5, 1, 12, 0, 0, tzinfo=UTC),
        payload=payload,
    )


async def _drain_idle(*, max_iters: int = 50) -> None:
    """Yield a few times so the snapshot drain task can pick up published
    messages. The bus is single-threaded asyncio so a handful of zero-second
    sleeps is plenty; we cap to avoid hangs if something is wrong."""
    for _ in range(max_iters):
        await asyncio.sleep(0)


# ---------------------------------------------------------------------------
# Lifecycle
# ---------------------------------------------------------------------------


async def test_start_is_idempotent() -> None:
    bus = InMemoryBus()
    store = SnapshotStore(bus)
    await store.start()
    first_task = store._task
    await store.start()
    assert store._task is first_task
    await store.stop()


async def test_stop_is_idempotent() -> None:
    bus = InMemoryBus()
    store = SnapshotStore(bus)
    await store.start()
    await store.stop()
    await store.stop()  # second call must not raise


async def test_track_untrack_before_start_is_noop() -> None:
    """Until start() allocates the subscription, tracking is a silent no-op
    rather than an error — keeps wiring code straight-line."""
    bus = InMemoryBus()
    store = SnapshotStore(bus)
    await store.track(TICKER_BTC)
    await store.untrack(TICKER_BTC)
    assert store.product_ids() == frozenset()


async def test_can_restart_after_stop() -> None:
    """Snapshot data persists across a stop/start cycle (it's a cache)."""
    bus = InMemoryBus()
    store = SnapshotStore(bus)
    await store.start()
    await store.track(TICKER_BTC)
    await bus.publish(_ticker(price="60000"))
    await _drain_idle()
    await store.stop()
    assert store.get("BTC-USD") is not None  # cache survives stop

    await store.start()
    await store.track(TICKER_BTC)
    await bus.publish(_ticker(price="61000"))
    await _drain_idle()
    snap = store.get("BTC-USD")
    assert snap is not None
    assert snap.last_ticker is not None
    assert snap.last_ticker.price == Decimal("61000")
    await store.stop()


# ---------------------------------------------------------------------------
# Payload dispatch — synthetic stream tests (the headline verify)
# ---------------------------------------------------------------------------


async def test_ticker_updates_last_ticker_and_top_of_book() -> None:
    bus = InMemoryBus()
    store = SnapshotStore(bus)
    await store.start()
    try:
        await store.track(TICKER_BTC)
        await bus.publish(
            _ticker(
                best_bid="49999.50",
                best_bid_size="1.25",
                best_ask="50000.50",
                best_ask_size="2.25",
            )
        )
        await _drain_idle()

        snap = store.get("BTC-USD")
        assert snap is not None
        assert snap.last_ticker is not None
        assert snap.best_bid == Decimal("49999.50")
        assert snap.best_bid_size == Decimal("1.25")
        assert snap.best_ask == Decimal("50000.50")
        assert snap.best_ask_size == Decimal("2.25")
    finally:
        await store.stop()


async def test_trade_updates_last_trade_only() -> None:
    """A Trade should populate last_trade and last_update_at but leave book
    fields untouched (they come from Ticker / L2Snapshot)."""
    bus = InMemoryBus()
    store = SnapshotStore(bus)
    await store.start()
    try:
        await store.track(MATCHES_BTC)
        await bus.publish(_trade(price="50500", size="0.75"))
        await _drain_idle()

        snap = store.get("BTC-USD")
        assert snap is not None
        assert snap.last_trade is not None
        assert snap.last_trade.price == Decimal("50500")
        assert snap.last_trade.size == Decimal("0.75")
        assert snap.last_ticker is None
        assert snap.best_bid is None
        assert snap.best_ask is None
    finally:
        await store.stop()


async def test_last_match_type_treated_as_trade() -> None:
    """``last_match`` is the catch-up frame on subscribe; its fields are
    identical to ``match`` and should populate last_trade just the same."""
    bus = InMemoryBus()
    store = SnapshotStore(bus)
    await store.start()
    try:
        await store.track(MATCHES_BTC)
        await bus.publish(_trade(type_="last_match"))
        await _drain_idle()

        snap = store.get("BTC-USD")
        assert snap is not None
        assert snap.last_trade is not None
        assert snap.last_trade.type == "last_match"
    finally:
        await store.stop()


async def test_l2_snapshot_sets_top_of_book_from_sorted_lists() -> None:
    bus = InMemoryBus()
    store = SnapshotStore(bus)
    await store.start()
    try:
        await store.track(LEVEL2_BTC)
        await bus.publish(
            _l2_snapshot(
                bids=[("49000", "1.0"), ("48999", "2.0"), ("48998", "3.0")],
                asks=[("51000", "1.0"), ("51001", "2.0"), ("51002", "3.0")],
            )
        )
        await _drain_idle()

        snap = store.get("BTC-USD")
        assert snap is not None
        assert snap.best_bid == Decimal("49000")  # max of bids
        assert snap.best_bid_size == Decimal("1.0")
        assert snap.best_ask == Decimal("51000")  # min of asks
        assert snap.best_ask_size == Decimal("1.0")
    finally:
        await store.stop()


async def test_l2_snapshot_picks_top_even_when_unsorted() -> None:
    """Defensive: even if the wire ordering changes, top-of-book must be
    correct. We pick max-bid / min-ask explicitly."""
    bus = InMemoryBus()
    store = SnapshotStore(bus)
    await store.start()
    try:
        await store.track(LEVEL2_BTC)
        await bus.publish(
            _l2_snapshot(
                bids=[("48998", "3.0"), ("49000", "1.0"), ("48999", "2.0")],
                asks=[("51002", "3.0"), ("51000", "1.0"), ("51001", "2.0")],
            )
        )
        await _drain_idle()

        snap = store.get("BTC-USD")
        assert snap is not None
        assert snap.best_bid == Decimal("49000")
        assert snap.best_ask == Decimal("51000")
    finally:
        await store.stop()


async def test_l2_update_refreshes_timestamp_without_changing_top() -> None:
    bus = InMemoryBus()
    store = SnapshotStore(bus)
    await store.start()
    try:
        await store.track(TICKER_BTC)
        await store.track(LEVEL2_BTC)
        # Seed top-of-book via ticker.
        ts_initial = datetime(2026, 5, 1, 12, 0, 0, tzinfo=UTC)
        await bus.publish(
            _ticker(best_bid="49999", best_ask="50001", received_at=ts_initial)
        )
        # Then deliver an l2 update with a later timestamp.
        ts_later = datetime(2026, 5, 1, 12, 0, 5, tzinfo=UTC)
        await bus.publish(_l2_update(received_at=ts_later))
        await _drain_idle()

        snap = store.get("BTC-USD")
        assert snap is not None
        assert snap.best_bid == Decimal("49999")  # unchanged
        assert snap.best_ask == Decimal("50001")  # unchanged
        assert snap.last_update_at == ts_later  # but the timestamp moved
    finally:
        await store.stop()


async def test_last_update_at_uses_message_received_at() -> None:
    bus = InMemoryBus()
    store = SnapshotStore(bus)
    await store.start()
    try:
        await store.track(TICKER_BTC)
        ts = datetime(2026, 5, 1, 12, 34, 56, tzinfo=UTC)
        await bus.publish(_ticker(received_at=ts))
        await _drain_idle()

        snap = store.get("BTC-USD")
        assert snap is not None
        assert snap.last_update_at == ts
    finally:
        await store.stop()


# ---------------------------------------------------------------------------
# Multi-symbol isolation
# ---------------------------------------------------------------------------


async def test_multiple_symbols_tracked_independently() -> None:
    bus = InMemoryBus()
    store = SnapshotStore(bus)
    await store.start()
    try:
        await store.track(TICKER_BTC)
        await store.track(TICKER_ETH)

        await bus.publish(_ticker(product_id="BTC-USD", best_bid="50000"))
        await bus.publish(_ticker(product_id="ETH-USD", best_bid="3000"))
        await _drain_idle()

        btc = store.get("BTC-USD")
        eth = store.get("ETH-USD")
        assert btc is not None and eth is not None
        assert btc.best_bid == Decimal("50000")
        assert eth.best_bid == Decimal("3000")
        assert store.product_ids() == frozenset({"BTC-USD", "ETH-USD"})
    finally:
        await store.stop()


# ---------------------------------------------------------------------------
# Track / untrack semantics
# ---------------------------------------------------------------------------


async def test_track_is_idempotent() -> None:
    bus = InMemoryBus()
    store = SnapshotStore(bus)
    await store.start()
    try:
        await store.track(TICKER_BTC)
        await store.track(TICKER_BTC)  # second call must not raise
        await bus.publish(_ticker())
        await _drain_idle()

        # Single message published, single update applied.
        assert store.get("BTC-USD") is not None
    finally:
        await store.stop()


async def test_untrack_is_idempotent() -> None:
    bus = InMemoryBus()
    store = SnapshotStore(bus)
    await store.start()
    try:
        await store.untrack(TICKER_BTC)  # not currently tracked
        await store.track(TICKER_BTC)
        await store.untrack(TICKER_BTC)
        await store.untrack(TICKER_BTC)  # second untrack also no-op
    finally:
        await store.stop()


async def test_messages_on_untracked_topic_are_ignored() -> None:
    bus = InMemoryBus()
    store = SnapshotStore(bus)
    await store.start()
    try:
        # Never call track() — store should see nothing.
        await bus.publish(_ticker())
        await _drain_idle()
        assert store.get("BTC-USD") is None
    finally:
        await store.stop()


async def test_untrack_retains_last_known_state() -> None:
    """The whole point of the cache: once we know BTC's top-of-book, that
    answer remains queryable even after demand drops to zero. ``last_update_at``
    lets the caller decide whether the data is too stale to use."""
    bus = InMemoryBus()
    store = SnapshotStore(bus)
    await store.start()
    try:
        await store.track(TICKER_BTC)
        await bus.publish(_ticker(best_bid="49999"))
        await _drain_idle()

        await store.untrack(TICKER_BTC)
        # Publish a follow-up — store should NOT see it now.
        await bus.publish(_ticker(best_bid="40000"))
        await _drain_idle()

        snap = store.get("BTC-USD")
        assert snap is not None
        assert snap.best_bid == Decimal("49999")  # frozen at last seen
    finally:
        await store.stop()


# ---------------------------------------------------------------------------
# Inspection
# ---------------------------------------------------------------------------


async def test_get_unknown_product_returns_none() -> None:
    bus = InMemoryBus()
    store = SnapshotStore(bus)
    await store.start()
    try:
        assert store.get("DOES-NOT-EXIST") is None
    finally:
        await store.stop()


async def test_all_returns_every_tracked_snapshot() -> None:
    bus = InMemoryBus()
    store = SnapshotStore(bus)
    await store.start()
    try:
        await store.track(TICKER_BTC)
        await store.track(TICKER_ETH)
        await bus.publish(_ticker(product_id="BTC-USD"))
        await bus.publish(_ticker(product_id="ETH-USD"))
        await _drain_idle()

        all_snaps = store.all()
        assert len(all_snaps) == 2
        assert {s.product_id for s in all_snaps} == {"BTC-USD", "ETH-USD"}
    finally:
        await store.stop()


def test_to_dict_preserves_decimal_precision_as_strings() -> None:
    """Decimals must serialise as strings end-to-end — float-coercion in JSON
    would corrupt prices at BTC-scale notionals."""
    snap = SymbolSnapshot(
        product_id="BTC-USD",
        best_bid=Decimal("49999.99999999"),
        best_bid_size=Decimal("1.23456789"),
        best_ask=Decimal("50000.00000001"),
        best_ask_size=Decimal("2.34567891"),
        last_update_at=datetime(2026, 5, 1, 12, 0, 0, tzinfo=UTC),
    )
    d = snap.to_dict()
    assert d["best_bid"] == "49999.99999999"
    assert d["best_bid_size"] == "1.23456789"
    assert d["best_ask"] == "50000.00000001"
    assert d["best_ask_size"] == "2.34567891"
    assert d["last_update_at"] == "2026-05-01T12:00:00+00:00"
    assert d["last_ticker"] is None
    assert d["last_trade"] is None


def test_to_dict_includes_full_payloads_when_present() -> None:
    payload = Ticker(
        type="ticker",
        product_id="BTC-USD",
        sequence=1,
        trade_id=1,
        time=datetime(2026, 5, 1, 12, 0, 0, tzinfo=UTC),
        price=Decimal("50000"),
        last_size=Decimal("0.01"),
        side="buy",
        best_bid=Decimal("49999"),
        best_bid_size=Decimal("1"),
        best_ask=Decimal("50001"),
        best_ask_size=Decimal("1"),
        open_24h=Decimal("48000"),
        high_24h=Decimal("51000"),
        low_24h=Decimal("47000"),
        volume_24h=Decimal("1000"),
        volume_30d=Decimal("30000"),
    )
    snap = SymbolSnapshot(product_id="BTC-USD", last_ticker=payload)
    d = snap.to_dict()
    assert d["last_ticker"] is not None
    assert d["last_ticker"]["price"] == "50000"


# ---------------------------------------------------------------------------
# Passive observer invariant — store must NOT affect registry refcounts
# ---------------------------------------------------------------------------


async def test_snapshot_store_does_not_create_registry_demand() -> None:
    """If the snapshot store created demand, the registry would never see a
    1→0 transition while the store was tracking — and ingest would never
    unsubscribe upstream. Pin the invariant explicitly: registry refcounts
    are driven solely by ``register_consumer``, not by the snapshot store."""
    bus = InMemoryBus()
    on_first_calls: list[str] = []
    on_last_calls: list[str] = []

    async def on_first(topic: str) -> None:
        on_first_calls.append(topic)

    async def on_last(topic: str) -> None:
        on_last_calls.append(topic)

    registry = Registry(bus, on_first_subscriber=on_first, on_last_unsubscriber=on_last)
    store = SnapshotStore(bus)
    await store.start()
    try:
        # Only the snapshot store is observing the topic — registry must
        # still report zero subscribers.
        await store.track(TICKER_BTC)
        assert registry.topic_subscribers(TICKER_BTC) == frozenset()
        assert on_first_calls == []

        # When a real consumer registers, refcount goes 0→1 normally.
        await registry.register_consumer("c1", [TICKER_BTC])
        assert on_first_calls == [TICKER_BTC]

        # And when it leaves, refcount goes 1→0 — the snapshot store does
        # not pin the topic open.
        await registry.unregister_consumer("c1")
        assert on_last_calls == [TICKER_BTC]
    finally:
        await store.stop()


# ---------------------------------------------------------------------------
# End-to-end: composed callbacks (the wiring __main__ uses)
# ---------------------------------------------------------------------------


async def test_composed_callbacks_track_and_untrack_with_registry_lifecycle() -> None:
    """Mirrors ``__main__``: the registry's first/last callbacks fan out to
    both an ingest stub AND the snapshot store, so the store sees a topic
    exactly while at least one downstream consumer wants it."""
    bus = InMemoryBus()
    store = SnapshotStore(bus)
    await store.start()
    try:

        async def on_first(topic: str) -> None:
            await store.track(topic)

        async def on_last(topic: str) -> None:
            await store.untrack(topic)

        registry = Registry(
            bus, on_first_subscriber=on_first, on_last_unsubscriber=on_last
        )

        # Consumer arrives → store should pick up the topic.
        await registry.register_consumer("c1", [TICKER_BTC])
        await bus.publish(_ticker(best_bid="49000"))
        await _drain_idle()
        assert store.get("BTC-USD") is not None

        # Consumer leaves → store stops observing future messages, but the
        # last seen state remains queryable.
        await registry.unregister_consumer("c1")
        await bus.publish(_ticker(best_bid="40000"))
        await _drain_idle()
        snap = store.get("BTC-USD")
        assert snap is not None
        assert snap.best_bid == Decimal("49000")
    finally:
        await store.stop()


# ---------------------------------------------------------------------------
# Drain robustness
# ---------------------------------------------------------------------------


async def test_drain_continues_after_per_message_apply_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """One bad message must not kill the drain loop. Force _apply to raise
    on the first call, succeed on the second; confirm the second is processed."""
    bus = InMemoryBus()
    store = SnapshotStore(bus)
    await store.start()
    try:
        await store.track(TICKER_BTC)
        original = store._apply
        calls = {"n": 0}

        def flaky_apply(msg: Message) -> None:
            calls["n"] += 1
            if calls["n"] == 1:
                raise RuntimeError("synthetic apply error")
            original(msg)

        monkeypatch.setattr(store, "_apply", flaky_apply)

        await bus.publish(_ticker(best_bid="49999"))
        await bus.publish(_ticker(best_bid="50500"))
        await _drain_idle()

        snap = store.get("BTC-USD")
        assert snap is not None
        assert snap.best_bid == Decimal("50500")  # second message landed
        assert calls["n"] == 2
    finally:
        await store.stop()
