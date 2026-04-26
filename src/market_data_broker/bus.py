"""In-memory pub/sub bus.

The bus is the routing fabric between ingestion (publishers) and consumers
(downstream WS / MCP / snapshot store). Topics are exact strings — no wildcards
— matching the venue.channel.product_id scheme defined in :mod:`topics`.

Per-consumer backpressure
-------------------------
Each subscription owns its own bounded :class:`asyncio.Queue`. When the queue is
full at publish-time the bus drops the *oldest* queued message and increments
``dropped_messages`` on that subscription. A slow consumer therefore cannot
stall publishers or starve peers — its own backlog gets trimmed.

Forcibly closing a subscription whose drop rate stays unhealthy is the
*registry's* job (Step 4), not the bus's. The bus only owns the accounting.
"""

from __future__ import annotations

import asyncio
import itertools
from collections.abc import Iterable
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .models import Message

DEFAULT_MAX_QUEUE = 1000

_consumer_id_counter = itertools.count(1)


class SubscriptionClosedError(RuntimeError):
    """Raised when an operation is attempted on a closed subscription."""


@dataclass(eq=False)
class Subscription:
    """A consumer's handle on the bus.

    Hold this reference, ``await sub.get()`` (or ``async for msg in sub``) for
    messages, and call :meth:`close` when done. The subscription is bound to
    its bus; topic membership is mutable via :meth:`add_topic` /
    :meth:`remove_topic`.
    """

    consumer_id: str
    _bus: InMemoryBus
    _queue: asyncio.Queue[Message]
    _topics: set[str] = field(default_factory=set)
    dropped_messages: int = 0
    _closed: bool = False

    @property
    def topics(self) -> frozenset[str]:
        """Snapshot of subscribed topics. Returned as frozenset to discourage
        external mutation — use :meth:`add_topic` / :meth:`remove_topic`."""
        return frozenset(self._topics)

    @property
    def closed(self) -> bool:
        return self._closed

    def qsize(self) -> int:
        return self._queue.qsize()

    async def get(self) -> Message:
        """Await the next message. Raises :class:`SubscriptionClosedError` if
        the subscription is closed and no buffered messages remain."""
        if self._closed and self._queue.empty():
            raise SubscriptionClosedError(f"subscription {self.consumer_id} is closed")
        return await self._queue.get()

    def __aiter__(self) -> Subscription:
        return self

    async def __anext__(self) -> Message:
        try:
            return await self.get()
        except SubscriptionClosedError as exc:
            raise StopAsyncIteration from exc

    def add_topic(self, topic: str) -> None:
        self._bus.add_topic(self, topic)

    def remove_topic(self, topic: str) -> None:
        self._bus.remove_topic(self, topic)

    def close(self) -> None:
        self._bus.unsubscribe(self)

    def stats(self) -> dict[str, object]:
        return {
            "consumer_id": self.consumer_id,
            "topics": sorted(self._topics),
            "queue_size": self._queue.qsize(),
            "dropped_messages": self.dropped_messages,
            "closed": self._closed,
        }


class InMemoryBus:
    """Process-local pub/sub. Single-threaded asyncio — no locks needed because
    publish/subscribe/unsubscribe never yield between table reads and writes."""

    def __init__(self, *, default_max_queue: int = DEFAULT_MAX_QUEUE) -> None:
        if default_max_queue <= 0:
            raise ValueError(f"default_max_queue must be > 0, got {default_max_queue}")
        self._default_max_queue = default_max_queue
        self._by_topic: dict[str, set[Subscription]] = {}
        self._subs: set[Subscription] = set()

    # -- subscription lifecycle -------------------------------------------------

    def subscribe(
        self,
        topics: Iterable[str] = (),
        *,
        consumer_id: str | None = None,
        max_queue: int | None = None,
    ) -> Subscription:
        """Create a new subscription. ``consumer_id`` is auto-generated if omitted.

        ``max_queue`` overrides the bus default for this subscription only;
        useful for known-bursty consumers (e.g. level2 snapshots can be ~50 KB
        each — give those a larger queue)."""
        topic_set = set(topics)
        cid = consumer_id or f"c{next(_consumer_id_counter)}"
        sub = Subscription(
            consumer_id=cid,
            _bus=self,
            _queue=asyncio.Queue(maxsize=max_queue or self._default_max_queue),
            _topics=topic_set,
        )
        self._subs.add(sub)
        for t in topic_set:
            self._by_topic.setdefault(t, set()).add(sub)
        return sub

    def unsubscribe(self, sub: Subscription) -> None:
        """Detach a subscription from all topics. Idempotent."""
        if sub._closed:
            return
        sub._closed = True
        for t in sub._topics:
            holders = self._by_topic.get(t)
            if holders is None:
                continue
            holders.discard(sub)
            if not holders:
                del self._by_topic[t]
        self._subs.discard(sub)

    def add_topic(self, sub: Subscription, topic: str) -> None:
        if sub._closed:
            raise SubscriptionClosedError(
                f"cannot add topic to closed subscription {sub.consumer_id}"
            )
        if topic in sub._topics:
            return
        sub._topics.add(topic)
        self._by_topic.setdefault(topic, set()).add(sub)

    def remove_topic(self, sub: Subscription, topic: str) -> None:
        if sub._closed:
            return
        if topic not in sub._topics:
            return
        sub._topics.discard(topic)
        holders = self._by_topic.get(topic)
        if holders is not None:
            holders.discard(sub)
            if not holders:
                del self._by_topic[topic]

    # -- publish ----------------------------------------------------------------

    async def publish(self, message: Message) -> None:
        """Fan a message out to every subscriber of its topic.

        ``async`` so callers can ``await bus.publish(msg)`` consistently — the
        body never yields, and all subscribers are notified atomically with
        respect to other coroutines.
        """
        # Iterate a copy: a consumer's closure callback (Step 4) might mutate
        # the holder set during fan-out. Today's bus doesn't do that, but the
        # cost of `list(...)` is trivial and the property is worth pinning.
        for sub in list(self._by_topic.get(message.topic, ())):
            self._deliver(sub, message)

    def _deliver(self, sub: Subscription, message: Message) -> None:
        try:
            sub._queue.put_nowait(message)
            return
        except asyncio.QueueFull:
            pass
        # Drop-oldest: pop one to make room, then put the new one.
        try:
            sub._queue.get_nowait()
        except asyncio.QueueEmpty:
            # Maxsize must be 0 (unbounded) — by construction we reject this in
            # __init__, so reaching here means an external race we cannot fix.
            sub.dropped_messages += 1
            return
        sub.dropped_messages += 1
        try:
            sub._queue.put_nowait(message)
        except asyncio.QueueFull:
            # Should be impossible single-threaded after a successful pop, but
            # be defensive: count the new message as dropped too.
            sub.dropped_messages += 1

    # -- introspection ----------------------------------------------------------

    def subscriber_count(self, topic: str) -> int:
        return len(self._by_topic.get(topic, ()))

    def topics(self) -> frozenset[str]:
        return frozenset(self._by_topic.keys())

    def stats(self) -> dict[str, object]:
        return {
            "subscriber_count": len(self._subs),
            "active_topics": sorted(self._by_topic.keys()),
            "subscribers": [sub.stats() for sub in self._subs],
        }
