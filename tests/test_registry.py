"""Tests for the connection registry.

Plan verifies: ref-count edge cases (0→1, 1→0, churn, leaks on disconnect).
We additionally cover: callback ordering, dynamic add/remove topic semantics,
upstream tracking, message-rate aggregation, and the status surface used by
``/status`` and the ``get_hub_status`` MCP tool.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from decimal import Decimal

import pytest

from market_data_broker.bus import InMemoryBus
from market_data_broker.models import Message, Ticker
from market_data_broker.registry import (
    ConsumerAlreadyRegisteredError,
    Registry,
    UnknownConsumerError,
    UpstreamAlreadyRegisteredError,
)

TOPIC_BTC = "coinbase.ticker.BTC-USD"
TOPIC_ETH = "coinbase.ticker.ETH-USD"
TOPIC_SOL = "coinbase.ticker.SOL-USD"


def _ticker_msg(topic: str = TOPIC_BTC) -> Message:
    """Bare-minimum Message for tests where the routing payload doesn't matter."""
    payload = Ticker(
        type="ticker",
        product_id="BTC-USD",
        sequence=1,
        trade_id=1,
        time=datetime(2026, 4, 26, 12, 0, 0, tzinfo=UTC),
        price=Decimal("50000"),
        last_size=Decimal("0.01"),
        side="buy",
        best_bid=Decimal("49999"),
        best_bid_size=Decimal("1.0"),
        best_ask=Decimal("50001"),
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
        received_at=datetime(2026, 4, 26, 12, 0, 0, tzinfo=UTC),
        payload=payload,
    )


class CallbackRecorder:
    """Async callback that records each topic it was fired for, in order."""

    def __init__(self) -> None:
        self.calls: list[str] = []

    async def __call__(self, topic: str) -> None:
        self.calls.append(topic)


# ---------------------------------------------------------------------------
# Ref-count edge cases — the headline plan-verify
# ---------------------------------------------------------------------------


async def test_first_subscriber_fires_on_zero_to_one() -> None:
    bus = InMemoryBus()
    on_first = CallbackRecorder()
    on_last = CallbackRecorder()
    reg = Registry(bus, on_first_subscriber=on_first, on_last_unsubscriber=on_last)

    await reg.register_consumer("c1", [TOPIC_BTC])

    assert on_first.calls == [TOPIC_BTC]
    assert on_last.calls == []
    assert reg.is_topic_active(TOPIC_BTC)
    assert reg.topic_subscribers(TOPIC_BTC) == frozenset({"c1"})


async def test_second_subscriber_does_not_refire_first_callback() -> None:
    bus = InMemoryBus()
    on_first = CallbackRecorder()
    reg = Registry(bus, on_first_subscriber=on_first)

    await reg.register_consumer("c1", [TOPIC_BTC])
    await reg.register_consumer("c2", [TOPIC_BTC])

    # Callback fires once — at the 0→1 transition only.
    assert on_first.calls == [TOPIC_BTC]
    assert reg.topic_subscribers(TOPIC_BTC) == frozenset({"c1", "c2"})


async def test_last_unsubscriber_fires_on_one_to_zero() -> None:
    bus = InMemoryBus()
    on_first = CallbackRecorder()
    on_last = CallbackRecorder()
    reg = Registry(bus, on_first_subscriber=on_first, on_last_unsubscriber=on_last)

    await reg.register_consumer("c1", [TOPIC_BTC])
    await reg.unregister_consumer("c1")

    assert on_first.calls == [TOPIC_BTC]
    assert on_last.calls == [TOPIC_BTC]
    assert not reg.is_topic_active(TOPIC_BTC)


async def test_non_last_unsubscriber_does_not_fire_last_callback() -> None:
    bus = InMemoryBus()
    on_last = CallbackRecorder()
    reg = Registry(bus, on_last_unsubscriber=on_last)

    await reg.register_consumer("c1", [TOPIC_BTC])
    await reg.register_consumer("c2", [TOPIC_BTC])
    await reg.unregister_consumer("c1")

    # c2 still holds it.
    assert on_last.calls == []
    assert reg.topic_subscribers(TOPIC_BTC) == frozenset({"c2"})


async def test_churn_zero_one_two_one_zero_fires_first_and_last_exactly_once() -> None:
    """Plan-named edge case: subscribe / subscribe / unsubscribe / unsubscribe.

    Callbacks must fire ONLY at the boundaries — once at 0→1 and once at 1→0,
    with the intermediate 1→2→1 transitions silent."""
    bus = InMemoryBus()
    on_first = CallbackRecorder()
    on_last = CallbackRecorder()
    reg = Registry(bus, on_first_subscriber=on_first, on_last_unsubscriber=on_last)

    await reg.register_consumer("a", [TOPIC_BTC])  # 0 → 1
    await reg.register_consumer("b", [TOPIC_BTC])  # 1 → 2 (silent)
    await reg.unregister_consumer("a")             # 2 → 1 (silent)
    await reg.unregister_consumer("b")             # 1 → 0

    assert on_first.calls == [TOPIC_BTC]
    assert on_last.calls == [TOPIC_BTC]


async def test_re_subscription_refires_first_callback() -> None:
    """After 1→0, a fresh subscribe is again 0→1 and must fire again."""
    bus = InMemoryBus()
    on_first = CallbackRecorder()
    on_last = CallbackRecorder()
    reg = Registry(bus, on_first_subscriber=on_first, on_last_unsubscriber=on_last)

    await reg.register_consumer("c1", [TOPIC_BTC])
    await reg.unregister_consumer("c1")
    await reg.register_consumer("c2", [TOPIC_BTC])

    assert on_first.calls == [TOPIC_BTC, TOPIC_BTC]
    assert on_last.calls == [TOPIC_BTC]


# ---------------------------------------------------------------------------
# Multi-topic registration
# ---------------------------------------------------------------------------


async def test_register_with_multiple_topics_fires_per_topic() -> None:
    bus = InMemoryBus()
    on_first = CallbackRecorder()
    reg = Registry(bus, on_first_subscriber=on_first)

    await reg.register_consumer("c1", [TOPIC_BTC, TOPIC_ETH])

    assert sorted(on_first.calls) == sorted([TOPIC_BTC, TOPIC_ETH])
    assert reg.is_topic_active(TOPIC_BTC)
    assert reg.is_topic_active(TOPIC_ETH)


async def test_register_with_duplicate_topics_dedupes() -> None:
    bus = InMemoryBus()
    on_first = CallbackRecorder()
    reg = Registry(bus, on_first_subscriber=on_first)

    await reg.register_consumer("c1", [TOPIC_BTC, TOPIC_BTC, TOPIC_BTC])
    # Callback fires once despite duplicate input.
    assert on_first.calls == [TOPIC_BTC]


async def test_unregister_with_multiple_topics_fires_per_topic_dropped() -> None:
    bus = InMemoryBus()
    on_last = CallbackRecorder()
    reg = Registry(bus, on_last_unsubscriber=on_last)

    await reg.register_consumer("c1", [TOPIC_BTC, TOPIC_ETH])
    await reg.unregister_consumer("c1")

    assert sorted(on_last.calls) == sorted([TOPIC_BTC, TOPIC_ETH])


# ---------------------------------------------------------------------------
# Dynamic add / remove topic
# ---------------------------------------------------------------------------


async def test_add_topic_fires_first_when_zero_to_one() -> None:
    bus = InMemoryBus()
    on_first = CallbackRecorder()
    reg = Registry(bus, on_first_subscriber=on_first)

    await reg.register_consumer("c1", [TOPIC_BTC])
    on_first.calls.clear()

    await reg.add_topic("c1", TOPIC_ETH)

    assert on_first.calls == [TOPIC_ETH]
    assert reg.topic_subscribers(TOPIC_ETH) == frozenset({"c1"})


async def test_add_topic_idempotent_does_not_refire() -> None:
    bus = InMemoryBus()
    on_first = CallbackRecorder()
    reg = Registry(bus, on_first_subscriber=on_first)

    await reg.register_consumer("c1", [TOPIC_BTC])
    on_first.calls.clear()

    await reg.add_topic("c1", TOPIC_BTC)  # already subscribed
    assert on_first.calls == []


async def test_remove_topic_fires_last_when_one_to_zero() -> None:
    bus = InMemoryBus()
    on_last = CallbackRecorder()
    reg = Registry(bus, on_last_unsubscriber=on_last)

    await reg.register_consumer("c1", [TOPIC_BTC, TOPIC_ETH])

    await reg.remove_topic("c1", TOPIC_BTC)
    assert on_last.calls == [TOPIC_BTC]
    assert not reg.is_topic_active(TOPIC_BTC)
    assert reg.is_topic_active(TOPIC_ETH)


async def test_remove_topic_silent_when_other_subscribers_remain() -> None:
    bus = InMemoryBus()
    on_last = CallbackRecorder()
    reg = Registry(bus, on_last_unsubscriber=on_last)

    await reg.register_consumer("c1", [TOPIC_BTC])
    await reg.register_consumer("c2", [TOPIC_BTC])

    await reg.remove_topic("c1", TOPIC_BTC)
    assert on_last.calls == []
    assert reg.topic_subscribers(TOPIC_BTC) == frozenset({"c2"})


async def test_remove_topic_unknown_topic_is_silent() -> None:
    bus = InMemoryBus()
    on_last = CallbackRecorder()
    reg = Registry(bus, on_last_unsubscriber=on_last)

    await reg.register_consumer("c1", [TOPIC_BTC])
    await reg.remove_topic("c1", TOPIC_ETH)  # never subscribed
    assert on_last.calls == []


async def test_add_topic_unknown_consumer_raises() -> None:
    reg = Registry(InMemoryBus())
    with pytest.raises(UnknownConsumerError):
        await reg.add_topic("ghost", TOPIC_BTC)


async def test_remove_topic_unknown_consumer_raises() -> None:
    reg = Registry(InMemoryBus())
    with pytest.raises(UnknownConsumerError):
        await reg.remove_topic("ghost", TOPIC_BTC)


# ---------------------------------------------------------------------------
# Consumer lifecycle errors
# ---------------------------------------------------------------------------


async def test_duplicate_consumer_id_rejected() -> None:
    reg = Registry(InMemoryBus())
    await reg.register_consumer("c1", [TOPIC_BTC])
    with pytest.raises(ConsumerAlreadyRegisteredError):
        await reg.register_consumer("c1", [TOPIC_ETH])


async def test_empty_consumer_id_rejected() -> None:
    reg = Registry(InMemoryBus())
    with pytest.raises(ValueError):
        await reg.register_consumer("", [TOPIC_BTC])


async def test_unregister_unknown_consumer_is_silent() -> None:
    """Idempotent teardown — Step 7's WS server will call this from disconnect
    handlers that may run twice on race-y socket closes."""
    on_last = CallbackRecorder()
    reg = Registry(InMemoryBus(), on_last_unsubscriber=on_last)
    await reg.unregister_consumer("ghost")  # must not raise
    assert on_last.calls == []


# ---------------------------------------------------------------------------
# No-leaks guarantee — the headline correctness property
# ---------------------------------------------------------------------------


async def test_full_lifecycle_no_leaks() -> None:
    """After 100 connect/disconnect cycles across several topics, the registry
    must hold zero refcounts and the bus must hold zero subscribers — confirming
    we don't leak ref-counts or bus subscriptions on disconnect.

    This is the property reviewers will probe most aggressively (CLAUDE.md
    explicitly calls leaked upstream subs out as a primary failure mode)."""
    bus = InMemoryBus()
    on_first = CallbackRecorder()
    on_last = CallbackRecorder()
    reg = Registry(bus, on_first_subscriber=on_first, on_last_unsubscriber=on_last)

    topics = [TOPIC_BTC, TOPIC_ETH, TOPIC_SOL]
    for i in range(100):
        cid = f"c{i}"
        await reg.register_consumer(cid, topics)
        await reg.unregister_consumer(cid)

    assert reg.active_topics() == frozenset()
    for topic in topics:
        assert reg.topic_subscribers(topic) == frozenset()
        assert bus.subscriber_count(topic) == 0
    # Each topic transitions 0→1→0 each cycle: 100 first + 100 last per topic.
    assert len(on_first.calls) == 100 * len(topics)
    assert len(on_last.calls) == 100 * len(topics)


async def test_returned_subscription_actually_receives_messages() -> None:
    """The registry must return a working Subscription, not a detached one.

    Catches a class of bug where the registry might mis-wire the bus call and
    hand back a Subscription that's registered nowhere."""
    bus = InMemoryBus()
    reg = Registry(bus)
    sub = await reg.register_consumer("c1", [TOPIC_BTC])

    msg = _ticker_msg()
    await bus.publish(msg)
    received = await asyncio.wait_for(sub.get(), timeout=1)
    assert received is msg


async def test_unregister_closes_underlying_subscription() -> None:
    bus = InMemoryBus()
    reg = Registry(bus)
    sub = await reg.register_consumer("c1", [TOPIC_BTC])
    assert bus.subscriber_count(TOPIC_BTC) == 1

    await reg.unregister_consumer("c1")

    assert bus.subscriber_count(TOPIC_BTC) == 0
    assert sub.closed is True


# ---------------------------------------------------------------------------
# Upstream tracking
# ---------------------------------------------------------------------------


def test_register_upstream_returns_record_with_initial_state() -> None:
    reg = Registry(InMemoryBus())
    conn = reg.register_upstream("coinbase")
    assert conn.name == "coinbase"
    assert conn.state == "connecting"
    assert conn.topics == set()
    assert conn.reconnect_count == 0
    assert reg.upstream("coinbase") is conn


def test_register_upstream_duplicate_rejected() -> None:
    reg = Registry(InMemoryBus())
    reg.register_upstream("coinbase")
    with pytest.raises(UpstreamAlreadyRegisteredError):
        reg.register_upstream("coinbase")


def test_upstream_state_transitions() -> None:
    reg = Registry(InMemoryBus())
    conn = reg.register_upstream("coinbase")

    conn.mark_connected()
    assert conn.state == "connected"
    assert conn.reconnect_count == 0  # first connect doesn't count as reconnect

    conn.mark_reconnecting()
    assert conn.state == "reconnecting"

    conn.mark_connected()
    assert conn.state == "connected"
    assert conn.reconnect_count == 1  # incremented on the connect-after-reconnect

    conn.mark_disconnected()
    assert conn.state == "disconnected"


def test_unregister_upstream_idempotent() -> None:
    reg = Registry(InMemoryBus())
    reg.register_upstream("coinbase")
    reg.unregister_upstream("coinbase")
    reg.unregister_upstream("coinbase")  # must not raise
    assert reg.upstream("coinbase") is None


# ---------------------------------------------------------------------------
# Message rate tracking
# ---------------------------------------------------------------------------


def test_record_message_increments_per_topic() -> None:
    reg = Registry(InMemoryBus())
    reg.record_message(TOPIC_BTC)
    reg.record_message(TOPIC_BTC)
    reg.record_message(TOPIC_ETH)
    status = reg.status()
    by_topic = {t["topic"]: t for t in status["topics"]}
    assert by_topic[TOPIC_BTC]["messages_total"] == 2
    assert by_topic[TOPIC_ETH]["messages_total"] == 1


# ---------------------------------------------------------------------------
# Status surface
# ---------------------------------------------------------------------------


async def test_status_reports_consumers_upstreams_and_topics() -> None:
    bus = InMemoryBus()
    reg = Registry(bus)
    reg.register_upstream("coinbase").mark_connected()
    await reg.register_consumer("c1", [TOPIC_BTC])
    await reg.register_consumer("c2", [TOPIC_BTC, TOPIC_ETH])
    reg.record_message(TOPIC_BTC)
    reg.record_message(TOPIC_BTC)

    status = reg.status()

    assert "hub_started_at" in status
    assert status["hub_uptime_seconds"] >= 0
    assert {u["name"] for u in status["upstreams"]} == {"coinbase"}
    assert {c["consumer_id"] for c in status["consumers"]} == {"c1", "c2"}

    by_topic = {t["topic"]: t for t in status["topics"]}
    assert by_topic[TOPIC_BTC]["subscriber_count"] == 2
    assert by_topic[TOPIC_BTC]["messages_total"] == 2
    assert by_topic[TOPIC_BTC]["messages_per_second"] >= 0
    assert by_topic[TOPIC_ETH]["subscriber_count"] == 1
    assert by_topic[TOPIC_ETH]["messages_total"] == 0


async def test_status_messages_per_second_uses_hub_uptime() -> None:
    """Use a frozen ``now`` so the rate computation is deterministic."""
    bus = InMemoryBus()
    reg = Registry(bus)
    for _ in range(20):
        reg.record_message(TOPIC_BTC)
    # 10 seconds after start → 20 msgs / 10s = 2.0 msgs/sec.
    later = datetime.now(UTC).replace(microsecond=0)
    later = later.fromtimestamp(reg._started_at.timestamp() + 10, tz=UTC)
    status = reg.status(now=later)
    by_topic = {t["topic"]: t for t in status["topics"]}
    assert by_topic[TOPIC_BTC]["messages_per_second"] == pytest.approx(2.0, rel=1e-3)


async def test_consumer_stats_reports_uptime_and_drops() -> None:
    bus = InMemoryBus(default_max_queue=1)
    reg = Registry(bus)
    sub = await reg.register_consumer("c1", [TOPIC_BTC])
    # Cause some drops.
    for _ in range(5):
        await bus.publish(_ticker_msg())
    assert sub.dropped_messages == 4

    consumer = reg.consumer("c1")
    assert consumer is not None
    stats = consumer.stats()
    assert stats["consumer_id"] == "c1"
    assert stats["topics"] == [TOPIC_BTC]
    assert stats["dropped_messages"] == 4
    assert stats["queue_size"] == 1
    assert stats["uptime_seconds"] >= 0


# ---------------------------------------------------------------------------
# Callback wiring
# ---------------------------------------------------------------------------


async def test_no_callbacks_configured_does_not_raise() -> None:
    """The registry must work without callbacks — Step 6 (snapshot store)
    will use it before Step 5 (ingest) wires callbacks."""
    reg = Registry(InMemoryBus())  # no callbacks at all
    await reg.register_consumer("c1", [TOPIC_BTC])
    await reg.add_topic("c1", TOPIC_ETH)
    await reg.remove_topic("c1", TOPIC_ETH)
    await reg.unregister_consumer("c1")
    # Reaching this line without raising is the assertion.


async def test_callbacks_invoked_in_topic_input_order() -> None:
    """Deterministic callback ordering — useful for tests and for ingest
    implementations that batch upstream subscribe frames."""
    on_first = CallbackRecorder()
    reg = Registry(InMemoryBus(), on_first_subscriber=on_first)
    await reg.register_consumer("c1", [TOPIC_SOL, TOPIC_ETH, TOPIC_BTC])
    assert on_first.calls == [TOPIC_SOL, TOPIC_ETH, TOPIC_BTC]
