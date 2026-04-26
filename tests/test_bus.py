"""Tests for the in-memory pub/sub bus.

Verify per the plan:
- multi-consumer fan-out
- slow-consumer drop-oldest behaviour with counter
- dynamic subscribe / unsubscribe
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from decimal import Decimal

import pytest

from market_data_broker.bus import (
    DEFAULT_MAX_QUEUE,
    InMemoryBus,
    SubscriptionClosedError,
)
from market_data_broker.models import Message, Ticker

TOPIC_BTC = "coinbase.ticker.BTC-USD"
TOPIC_ETH = "coinbase.ticker.ETH-USD"


def _ticker_msg(topic: str = TOPIC_BTC, *, price: str = "50000.00") -> Message:
    """Build a minimal valid Message for tests. Field values don't matter for
    bus routing — only ``topic`` does — but we use the real model so any
    incidental contract drift surfaces here too."""
    payload = Ticker(
        type="ticker",
        product_id="BTC-USD",
        sequence=1,
        trade_id=1,
        time=datetime(2026, 4, 25, 12, 0, 0, tzinfo=UTC),
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
        received_at=datetime(2026, 4, 25, 12, 0, 0, tzinfo=UTC),
        payload=payload,
    )


# ---------------------------------------------------------------------------
# Construction & basic invariants
# ---------------------------------------------------------------------------


def test_default_max_queue_is_1000() -> None:
    """Plan pins the default at 1000."""
    assert DEFAULT_MAX_QUEUE == 1000


def test_invalid_default_max_queue_rejected() -> None:
    with pytest.raises(ValueError):
        InMemoryBus(default_max_queue=0)
    with pytest.raises(ValueError):
        InMemoryBus(default_max_queue=-1)


def test_subscribe_auto_assigns_consumer_id() -> None:
    bus = InMemoryBus()
    a = bus.subscribe([TOPIC_BTC])
    b = bus.subscribe([TOPIC_BTC])
    assert a.consumer_id != b.consumer_id


def test_subscribe_respects_explicit_consumer_id() -> None:
    bus = InMemoryBus()
    sub = bus.subscribe([TOPIC_BTC], consumer_id="downstream-1")
    assert sub.consumer_id == "downstream-1"


def test_initial_topic_set_is_exposed() -> None:
    bus = InMemoryBus()
    sub = bus.subscribe([TOPIC_BTC, TOPIC_ETH])
    assert sub.topics == frozenset({TOPIC_BTC, TOPIC_ETH})


# ---------------------------------------------------------------------------
# Routing & fan-out
# ---------------------------------------------------------------------------


async def test_publish_with_no_subscribers_is_noop() -> None:
    bus = InMemoryBus()
    # Must not raise even when no one is listening.
    await bus.publish(_ticker_msg())


async def test_single_subscriber_receives_message() -> None:
    bus = InMemoryBus()
    sub = bus.subscribe([TOPIC_BTC])
    msg = _ticker_msg()

    await bus.publish(msg)
    received = await asyncio.wait_for(sub.get(), timeout=1)
    assert received is msg


async def test_multi_consumer_fanout_each_receives_independent_copy() -> None:
    """N subscribers on the same topic each see every message.

    They share the same Message object (intentional — messages are pydantic
    frozen models, immutable) but each has an independent queue position."""
    bus = InMemoryBus()
    subs = [bus.subscribe([TOPIC_BTC]) for _ in range(5)]

    msg1 = _ticker_msg(price="100")
    msg2 = _ticker_msg(price="200")
    await bus.publish(msg1)
    await bus.publish(msg2)

    for sub in subs:
        m1 = await asyncio.wait_for(sub.get(), timeout=1)
        m2 = await asyncio.wait_for(sub.get(), timeout=1)
        assert m1 is msg1
        assert m2 is msg2
        assert sub.qsize() == 0


async def test_topic_filtering_only_matching_topic_is_delivered() -> None:
    bus = InMemoryBus()
    btc_sub = bus.subscribe([TOPIC_BTC])
    eth_sub = bus.subscribe([TOPIC_ETH])

    btc_msg = _ticker_msg(topic=TOPIC_BTC)
    eth_msg = _ticker_msg(topic=TOPIC_ETH)
    await bus.publish(btc_msg)
    await bus.publish(eth_msg)

    assert (await asyncio.wait_for(btc_sub.get(), timeout=1)) is btc_msg
    assert (await asyncio.wait_for(eth_sub.get(), timeout=1)) is eth_msg
    assert btc_sub.qsize() == 0
    assert eth_sub.qsize() == 0


async def test_unknown_topic_publish_is_silent() -> None:
    bus = InMemoryBus()
    sub = bus.subscribe([TOPIC_BTC])
    await bus.publish(_ticker_msg(topic="coinbase.ticker.SOL-USD"))
    assert sub.qsize() == 0


# ---------------------------------------------------------------------------
# Backpressure: drop-oldest + counter
# ---------------------------------------------------------------------------


async def test_slow_consumer_drops_oldest_when_queue_full() -> None:
    """Plan: bounded queue, drop-oldest, increment counter.

    Verifies both the count and which messages survive: when capacity=3 and we
    publish 5 in a row without draining, the queue should hold the *last* 3
    and ``dropped_messages == 2``."""
    bus = InMemoryBus(default_max_queue=3)
    sub = bus.subscribe([TOPIC_BTC])

    msgs = [_ticker_msg(price=str(i)) for i in range(5)]
    for m in msgs:
        await bus.publish(m)

    assert sub.qsize() == 3
    assert sub.dropped_messages == 2

    # The two oldest (0, 1) are gone; what remains is (2, 3, 4) in order.
    received = [await sub.get() for _ in range(3)]
    assert [m.payload.price for m in received] == [Decimal("2"), Decimal("3"), Decimal("4")]


async def test_slow_consumer_does_not_block_peers() -> None:
    """A consumer that never drains its queue must not stall fan-out to
    healthy peers — the whole point of per-consumer queues."""
    bus = InMemoryBus(default_max_queue=2)
    slow = bus.subscribe([TOPIC_BTC], consumer_id="slow")
    # ``fast`` opts in to a larger queue so we can verify it received every
    # message regardless of ``slow``'s saturation.
    fast = bus.subscribe([TOPIC_BTC], consumer_id="fast", max_queue=100)

    # Publish 10 messages; ``slow`` never reads.
    for i in range(10):
        await bus.publish(_ticker_msg(price=str(i)))

    # ``fast`` drains all 10 in order — slow's queue saturation didn't matter.
    received = [await asyncio.wait_for(fast.get(), timeout=1) for _ in range(10)]
    assert [m.payload.price for m in received] == [Decimal(str(i)) for i in range(10)]

    # ``slow`` only kept the last 2 and recorded 8 drops.
    assert slow.qsize() == 2
    assert slow.dropped_messages == 8


async def test_per_subscription_max_queue_override() -> None:
    bus = InMemoryBus(default_max_queue=2)
    big = bus.subscribe([TOPIC_BTC], max_queue=100, consumer_id="big")
    small = bus.subscribe([TOPIC_BTC], consumer_id="small")  # uses default 2

    for i in range(10):
        await bus.publish(_ticker_msg(price=str(i)))

    assert big.qsize() == 10
    assert big.dropped_messages == 0
    assert small.qsize() == 2
    assert small.dropped_messages == 8


# ---------------------------------------------------------------------------
# Dynamic subscribe / unsubscribe
# ---------------------------------------------------------------------------


async def test_unsubscribe_stops_delivery() -> None:
    bus = InMemoryBus()
    sub = bus.subscribe([TOPIC_BTC])

    await bus.publish(_ticker_msg(price="1"))
    bus.unsubscribe(sub)
    await bus.publish(_ticker_msg(price="2"))

    # Pre-close message survived in the queue.
    pre = await asyncio.wait_for(sub.get(), timeout=1)
    assert pre.payload.price == Decimal("1")
    # Post-close publish was not delivered.
    assert sub.qsize() == 0


async def test_unsubscribe_idempotent() -> None:
    bus = InMemoryBus()
    sub = bus.subscribe([TOPIC_BTC])
    bus.unsubscribe(sub)
    bus.unsubscribe(sub)  # must not raise
    assert sub.closed is True


async def test_close_via_subscription_method() -> None:
    bus = InMemoryBus()
    sub = bus.subscribe([TOPIC_BTC])
    sub.close()
    assert sub.closed is True
    assert bus.subscriber_count(TOPIC_BTC) == 0


async def test_dynamic_add_topic() -> None:
    bus = InMemoryBus()
    sub = bus.subscribe([TOPIC_BTC])

    # Initially not subscribed to ETH.
    await bus.publish(_ticker_msg(topic=TOPIC_ETH))
    assert sub.qsize() == 0

    sub.add_topic(TOPIC_ETH)
    eth_msg = _ticker_msg(topic=TOPIC_ETH)
    await bus.publish(eth_msg)
    assert (await asyncio.wait_for(sub.get(), timeout=1)) is eth_msg


async def test_dynamic_remove_topic() -> None:
    bus = InMemoryBus()
    sub = bus.subscribe([TOPIC_BTC, TOPIC_ETH])

    sub.remove_topic(TOPIC_BTC)
    assert sub.topics == frozenset({TOPIC_ETH})

    await bus.publish(_ticker_msg(topic=TOPIC_BTC))
    assert sub.qsize() == 0

    eth_msg = _ticker_msg(topic=TOPIC_ETH)
    await bus.publish(eth_msg)
    assert (await asyncio.wait_for(sub.get(), timeout=1)) is eth_msg


async def test_add_topic_is_idempotent() -> None:
    bus = InMemoryBus()
    sub = bus.subscribe([TOPIC_BTC])
    sub.add_topic(TOPIC_BTC)  # already there
    assert sub.topics == frozenset({TOPIC_BTC})

    msg = _ticker_msg()
    await bus.publish(msg)
    # Delivered exactly once, not twice.
    assert sub.qsize() == 1


async def test_remove_topic_unknown_is_idempotent() -> None:
    bus = InMemoryBus()
    sub = bus.subscribe([TOPIC_BTC])
    sub.remove_topic(TOPIC_ETH)  # never subscribed
    assert sub.topics == frozenset({TOPIC_BTC})


def test_add_topic_after_close_raises() -> None:
    bus = InMemoryBus()
    sub = bus.subscribe([TOPIC_BTC])
    sub.close()
    with pytest.raises(SubscriptionClosedError):
        sub.add_topic(TOPIC_ETH)


def test_remove_topic_after_close_is_silent() -> None:
    bus = InMemoryBus()
    sub = bus.subscribe([TOPIC_BTC])
    sub.close()
    sub.remove_topic(TOPIC_BTC)  # must not raise


# ---------------------------------------------------------------------------
# Async iteration
# ---------------------------------------------------------------------------


async def test_async_iter_drains_queue_then_stops_on_close() -> None:
    bus = InMemoryBus()
    sub = bus.subscribe([TOPIC_BTC])

    await bus.publish(_ticker_msg(price="1"))
    await bus.publish(_ticker_msg(price="2"))
    sub.close()

    # Closed but queue non-empty: iteration drains, then stops cleanly.
    received: list[Decimal] = []
    async for msg in sub:
        received.append(msg.payload.price)
    assert received == [Decimal("1"), Decimal("2")]


async def test_get_after_close_with_empty_queue_raises() -> None:
    bus = InMemoryBus()
    sub = bus.subscribe([TOPIC_BTC])
    sub.close()
    with pytest.raises(SubscriptionClosedError):
        await sub.get()


# ---------------------------------------------------------------------------
# Introspection
# ---------------------------------------------------------------------------


def test_subscriber_count_tracks_holders() -> None:
    bus = InMemoryBus()
    assert bus.subscriber_count(TOPIC_BTC) == 0

    a = bus.subscribe([TOPIC_BTC])
    b = bus.subscribe([TOPIC_BTC])
    assert bus.subscriber_count(TOPIC_BTC) == 2

    a.close()
    assert bus.subscriber_count(TOPIC_BTC) == 1

    b.remove_topic(TOPIC_BTC)
    assert bus.subscriber_count(TOPIC_BTC) == 0
    # Empty topic should be cleaned out of the routing table.
    assert TOPIC_BTC not in bus.topics()


def test_stats_reports_aggregate_view() -> None:
    bus = InMemoryBus()
    bus.subscribe([TOPIC_BTC], consumer_id="a")
    bus.subscribe([TOPIC_BTC, TOPIC_ETH], consumer_id="b")

    stats = bus.stats()
    assert stats["subscriber_count"] == 2
    assert sorted(stats["active_topics"]) == sorted([TOPIC_BTC, TOPIC_ETH])
    by_id = {s["consumer_id"]: s for s in stats["subscribers"]}
    assert by_id["a"]["topics"] == [TOPIC_BTC]
    assert by_id["b"]["topics"] == sorted([TOPIC_BTC, TOPIC_ETH])
    assert by_id["a"]["dropped_messages"] == 0


async def test_subscription_stats_reports_drops() -> None:
    bus = InMemoryBus(default_max_queue=1)
    sub = bus.subscribe([TOPIC_BTC], consumer_id="laggy")
    for i in range(4):
        await bus.publish(_ticker_msg(price=str(i)))

    stats = sub.stats()
    assert stats["consumer_id"] == "laggy"
    assert stats["dropped_messages"] == 3
    assert stats["queue_size"] == 1
    assert stats["closed"] is False


# ---------------------------------------------------------------------------
# Real-time delivery
# ---------------------------------------------------------------------------


async def test_consumer_awaiting_get_is_woken_by_publish() -> None:
    """Verify the asyncio.Queue handoff really wakes a parked consumer —
    catches subtle bugs where we accidentally bypass the queue mechanism."""
    bus = InMemoryBus()
    sub = bus.subscribe([TOPIC_BTC])

    msg = _ticker_msg()

    async def producer() -> None:
        # Yield control so the consumer task gets to await sub.get() first.
        await asyncio.sleep(0)
        await bus.publish(msg)

    received_first, _ = await asyncio.gather(
        asyncio.wait_for(sub.get(), timeout=1),
        producer(),
    )
    assert received_first is msg
