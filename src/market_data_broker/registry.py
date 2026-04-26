"""Connection registry: ref-counts downstream subscribers, tracks upstream
connections, and surfaces hub status.

Why this layer exists
---------------------
The bus knows *who is subscribed to what* but it does not know *whether anyone
still cares about a given topic*. The registry adds that ref-counting and
fires:

- ``on_first_subscriber(topic)`` — when a topic transitions ``0 → 1`` holders.
  The ingestion layer (Step 5) wires this to "send a SUBSCRIBE frame upstream".
- ``on_last_unsubscriber(topic)`` — when a topic transitions ``1 → 0`` holders.
  The ingestion layer wires this to "send an UNSUBSCRIBE frame upstream".

This is the mechanism that makes the hub demand-driven: we only consume from
Coinbase what at least one downstream consumer is actually using. Leaked
upstream subscriptions are a primary failure mode reviewers will probe — see
the test ``test_full_lifecycle_no_leaks`` for the explicit guarantee.

The bus stays oblivious to all of this; the registry is the *only* thing that
should fire ingest callbacks. Consumers must therefore go through the registry
(``register_consumer`` / ``add_topic``) rather than calling ``bus.subscribe``
directly — otherwise the ref-counts diverge from reality.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Iterable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from .bus import InMemoryBus, Subscription

# Async because the natural impl (sending an upstream WS frame) is async; even
# sync callbacks can be wrapped trivially in an ``async def`` shim.
TopicCallback = Callable[[str], Awaitable[None]]


class ConsumerAlreadyRegisteredError(ValueError):
    """Raised when ``register_consumer`` is given an in-use ``consumer_id``."""


class UnknownConsumerError(KeyError):
    """Raised when an operation references a ``consumer_id`` that is not registered."""


class UpstreamAlreadyRegisteredError(ValueError):
    """Raised when ``register_upstream`` is given an in-use upstream name."""


@dataclass(eq=False)
class ConsumerEntry:
    """Per-consumer record. ``connected_at`` drives uptime in status output."""

    consumer_id: str
    subscription: Subscription
    connected_at: datetime

    @property
    def topics(self) -> frozenset[str]:
        return self.subscription.topics

    def uptime_seconds(self, *, now: datetime | None = None) -> float:
        return ((now or datetime.now(UTC)) - self.connected_at).total_seconds()

    def stats(self, *, now: datetime | None = None) -> dict[str, Any]:
        return {
            "consumer_id": self.consumer_id,
            "topics": sorted(self.subscription.topics),
            "connected_at": self.connected_at.isoformat(),
            "uptime_seconds": round(self.uptime_seconds(now=now), 3),
            "queue_size": self.subscription.qsize(),
            "dropped_messages": self.subscription.dropped_messages,
        }


@dataclass
class UpstreamConnection:
    """Mutable record of an upstream venue connection.

    The registry stores it; the ingestion layer mutates ``state`` and ``topics``
    as connection events happen (connect / disconnect / reconnect / re-subscribe).
    Kept deliberately small — Step 5 will add ingest-specific concerns there.
    """

    name: str
    connected_at: datetime
    state: str = "connecting"  # "connecting" | "connected" | "reconnecting" | "disconnected"
    topics: set[str] = field(default_factory=set)
    last_event_at: datetime | None = None
    reconnect_count: int = 0

    def mark_connected(self, *, now: datetime | None = None) -> None:
        ts = now or datetime.now(UTC)
        if self.state == "reconnecting":
            self.reconnect_count += 1
        self.state = "connected"
        self.last_event_at = ts

    def mark_reconnecting(self, *, now: datetime | None = None) -> None:
        self.state = "reconnecting"
        self.last_event_at = now or datetime.now(UTC)

    def mark_disconnected(self, *, now: datetime | None = None) -> None:
        self.state = "disconnected"
        self.last_event_at = now or datetime.now(UTC)

    def uptime_seconds(self, *, now: datetime | None = None) -> float:
        return ((now or datetime.now(UTC)) - self.connected_at).total_seconds()

    def stats(self, *, now: datetime | None = None) -> dict[str, Any]:
        return {
            "name": self.name,
            "state": self.state,
            "connected_at": self.connected_at.isoformat(),
            "uptime_seconds": round(self.uptime_seconds(now=now), 3),
            "topics": sorted(self.topics),
            "reconnect_count": self.reconnect_count,
            "last_event_at": self.last_event_at.isoformat() if self.last_event_at else None,
        }


class Registry:
    """Ref-counted subscription tracker + upstream connection bookkeeper."""

    def __init__(
        self,
        bus: InMemoryBus,
        *,
        on_first_subscriber: TopicCallback | None = None,
        on_last_unsubscriber: TopicCallback | None = None,
    ) -> None:
        self._bus = bus
        self._on_first = on_first_subscriber
        self._on_last = on_last_unsubscriber

        self._consumers: dict[str, ConsumerEntry] = {}
        # topic → set of consumer_ids holding it. Empty sets are pruned so that
        # ``topic in self._refcount`` is a true "is anyone subscribed?" check.
        self._refcount: dict[str, set[str]] = {}
        self._upstreams: dict[str, UpstreamConnection] = {}
        self._message_counts: dict[str, int] = {}
        self._started_at: datetime = datetime.now(UTC)

    # ------------------------------------------------------------------
    # Downstream consumer lifecycle
    # ------------------------------------------------------------------

    async def register_consumer(
        self,
        consumer_id: str,
        topics: Iterable[str] = (),
        *,
        max_queue: int | None = None,
    ) -> Subscription:
        """Allocate a Subscription on the bus and start tracking the consumer.

        Fires ``on_first_subscriber(topic)`` once per topic that this consumer
        is the first to want. Returns the Subscription handle for reading.
        """
        if not consumer_id:
            raise ValueError("consumer_id must be a non-empty string")
        if consumer_id in self._consumers:
            raise ConsumerAlreadyRegisteredError(
                f"consumer_id {consumer_id!r} is already registered"
            )

        topic_set = list(dict.fromkeys(topics))  # dedupe, preserve order for callback firing
        sub = self._bus.subscribe(topic_set, consumer_id=consumer_id, max_queue=max_queue)
        self._consumers[consumer_id] = ConsumerEntry(
            consumer_id=consumer_id,
            subscription=sub,
            connected_at=datetime.now(UTC),
        )
        # Fire callbacks in deterministic order — useful for tests and for
        # ingest implementations that batch upstream subscribe frames.
        for topic in topic_set:
            if self._increment_refcount(topic, consumer_id):
                await self._fire_first(topic)
        return sub

    async def unregister_consumer(self, consumer_id: str) -> None:
        """Tear down a consumer. Fires ``on_last_unsubscriber`` for every topic
        this consumer was the last holder of. Idempotent: unknown ids are no-ops."""
        entry = self._consumers.pop(consumer_id, None)
        if entry is None:
            return
        # Snapshot before close so we can drive callbacks; the subscription's
        # topic view will be cleared as part of bus.unsubscribe.
        held_topics = list(entry.subscription.topics)
        entry.subscription.close()
        for topic in held_topics:
            if self._decrement_refcount(topic, consumer_id):
                await self._fire_last(topic)

    async def add_topic(self, consumer_id: str, topic: str) -> None:
        """Add a topic to an already-registered consumer. May fire ``on_first_subscriber``."""
        entry = self._require_consumer(consumer_id)
        if topic in entry.subscription.topics:
            return
        entry.subscription.add_topic(topic)
        if self._increment_refcount(topic, consumer_id):
            await self._fire_first(topic)

    async def remove_topic(self, consumer_id: str, topic: str) -> None:
        """Remove a topic from a registered consumer. May fire ``on_last_unsubscriber``."""
        entry = self._require_consumer(consumer_id)
        if topic not in entry.subscription.topics:
            return
        entry.subscription.remove_topic(topic)
        if self._decrement_refcount(topic, consumer_id):
            await self._fire_last(topic)

    # ------------------------------------------------------------------
    # Upstream tracking
    # ------------------------------------------------------------------

    def register_upstream(self, name: str) -> UpstreamConnection:
        if name in self._upstreams:
            raise UpstreamAlreadyRegisteredError(f"upstream {name!r} is already registered")
        conn = UpstreamConnection(name=name, connected_at=datetime.now(UTC))
        self._upstreams[name] = conn
        return conn

    def unregister_upstream(self, name: str) -> None:
        self._upstreams.pop(name, None)

    def upstream(self, name: str) -> UpstreamConnection | None:
        return self._upstreams.get(name)

    # ------------------------------------------------------------------
    # Message rate tracking
    # ------------------------------------------------------------------

    def record_message(self, topic: str) -> None:
        """Called by the ingestion layer on each successful publish.

        Kept on the registry rather than the bus so the bus stays a pure routing
        primitive; the registry is the natural home for hub-level observability.
        """
        self._message_counts[topic] = self._message_counts.get(topic, 0) + 1

    # ------------------------------------------------------------------
    # Inspection
    # ------------------------------------------------------------------

    def topic_subscribers(self, topic: str) -> frozenset[str]:
        return frozenset(self._refcount.get(topic, ()))

    def is_topic_active(self, topic: str) -> bool:
        return topic in self._refcount

    def active_topics(self) -> frozenset[str]:
        return frozenset(self._refcount.keys())

    def consumer(self, consumer_id: str) -> ConsumerEntry | None:
        return self._consumers.get(consumer_id)

    def hub_uptime_seconds(self, *, now: datetime | None = None) -> float:
        return ((now or datetime.now(UTC)) - self._started_at).total_seconds()

    def status(self, *, now: datetime | None = None) -> dict[str, Any]:
        ref_now = now or datetime.now(UTC)
        uptime = max((ref_now - self._started_at).total_seconds(), 1e-9)
        per_topic = []
        for topic in sorted(self._refcount.keys() | self._message_counts.keys()):
            count = self._message_counts.get(topic, 0)
            per_topic.append(
                {
                    "topic": topic,
                    "subscriber_count": len(self._refcount.get(topic, ())),
                    "messages_total": count,
                    "messages_per_second": round(count / uptime, 3),
                }
            )
        return {
            "hub_started_at": self._started_at.isoformat(),
            "hub_uptime_seconds": round(uptime, 3),
            "upstreams": [u.stats(now=ref_now) for u in self._upstreams.values()],
            "consumers": [c.stats(now=ref_now) for c in self._consumers.values()],
            "topics": per_topic,
        }

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _require_consumer(self, consumer_id: str) -> ConsumerEntry:
        entry = self._consumers.get(consumer_id)
        if entry is None:
            raise UnknownConsumerError(f"no consumer registered with id {consumer_id!r}")
        return entry

    def _increment_refcount(self, topic: str, consumer_id: str) -> bool:
        """Add holder; return True iff this transitioned topic from 0 → 1."""
        holders = self._refcount.get(topic)
        if holders is None:
            self._refcount[topic] = {consumer_id}
            return True
        was_empty = not holders  # defensive: pruning should keep this False
        holders.add(consumer_id)
        return was_empty

    def _decrement_refcount(self, topic: str, consumer_id: str) -> bool:
        """Remove holder; return True iff this transitioned topic from 1 → 0."""
        holders = self._refcount.get(topic)
        if holders is None:
            return False
        holders.discard(consumer_id)
        if not holders:
            del self._refcount[topic]
            return True
        return False

    async def _fire_first(self, topic: str) -> None:
        if self._on_first is not None:
            await self._on_first(topic)

    async def _fire_last(self, topic: str) -> None:
        if self._on_last is not None:
            await self._on_last(topic)
