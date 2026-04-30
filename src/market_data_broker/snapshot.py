"""Snapshot store — keeps the most recent market state per symbol.

The bus is a streaming primitive: a consumer that arrives mid-flight has no
way to recover the *current* state of a symbol without waiting for the next
tick. The snapshot store fills that gap by passively observing every message
that flows through tracked topics and caching the latest tick / trade /
book-top per ``product_id``.

Design notes
------------

- The store subscribes to the bus directly, NOT through the registry. Going
  through the registry would create artificial demand on every tracked topic
  and prevent ingest from ever unsubscribing upstream when downstream demand
  drops to zero. The store is a passive observer; demand is driven solely by
  real consumers.

- Topic tracking is dynamic via :meth:`track` / :meth:`untrack`. The natural
  wiring is to compose these with the registry's ``on_first_subscriber`` /
  ``on_last_unsubscriber`` callbacks so the store sees a topic exactly while
  at least one downstream consumer wants it. ``__main__`` does this.

- Snapshot data is RETAINED after :meth:`untrack` — ``get`` still works for
  symbols whose feeds went idle. Last known state is more useful to an LLM
  agent than a missing record; ``last_update_at`` lets the caller judge
  staleness.

- Best bid/ask is sourced from :class:`Ticker` (top-of-book post-trade) and
  :class:`L2Snapshot` (initial book on subscribe). Incremental
  :class:`L2Update` frames refresh ``last_update_at`` only — full book
  reconstruction is out of scope for this hub.

- Snapshots are keyed by ``product_id`` only. With a single venue today this
  is unambiguous; once a second venue is added, two venues quoting the same
  product would collide and the key should grow to ``(venue, product_id)``.
"""

from __future__ import annotations

import asyncio
import contextlib
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Any

from .bus import InMemoryBus, Subscription
from .logging_config import get_logger
from .models import L2Snapshot, L2Update, Message, Ticker, Trade

CONSUMER_ID = "snapshot-store"
# L2 snapshots are ~50 KB each and arrive in bursts on (re)subscribe — give
# the queue plenty of slack so the drain loop never trims fresh state.
DEFAULT_MAX_QUEUE = 4096


@dataclass
class SymbolSnapshot:
    """Latest known state for one ``product_id``.

    Fields are populated lazily as messages of the relevant type arrive — a
    symbol that only flows on the matches feed will have ``last_trade`` set
    but ``last_ticker`` and ``best_bid``/``best_ask`` left at ``None``.
    """

    product_id: str
    last_ticker: Ticker | None = None
    last_trade: Trade | None = None
    best_bid: Decimal | None = None
    best_bid_size: Decimal | None = None
    best_ask: Decimal | None = None
    best_ask_size: Decimal | None = None
    last_update_at: datetime | None = None

    def to_dict(self) -> dict[str, Any]:
        """JSON-friendly view. Decimals serialised as strings to preserve
        precision (the same reason models use Decimal end-to-end)."""
        return {
            "product_id": self.product_id,
            "last_ticker": (
                self.last_ticker.model_dump(mode="json") if self.last_ticker else None
            ),
            "last_trade": (
                self.last_trade.model_dump(mode="json") if self.last_trade else None
            ),
            "best_bid": str(self.best_bid) if self.best_bid is not None else None,
            "best_bid_size": (
                str(self.best_bid_size) if self.best_bid_size is not None else None
            ),
            "best_ask": str(self.best_ask) if self.best_ask is not None else None,
            "best_ask_size": (
                str(self.best_ask_size) if self.best_ask_size is not None else None
            ),
            "last_update_at": (
                self.last_update_at.isoformat() if self.last_update_at else None
            ),
        }


class SnapshotStore:
    """In-memory cache of the latest tick / trade / book-top per symbol.

    Lifecycle::

        store = SnapshotStore(bus)
        await store.start()
        # wire store.track / store.untrack into registry callbacks
        ...
        await store.stop()
    """

    def __init__(self, bus: InMemoryBus, *, max_queue: int = DEFAULT_MAX_QUEUE) -> None:
        self._bus = bus
        self._max_queue = max_queue
        self._sub: Subscription | None = None
        self._task: asyncio.Task[None] | None = None
        self._snapshots: dict[str, SymbolSnapshot] = {}
        self._log = get_logger("snapshot")

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def start(self) -> None:
        """Allocate the bus subscription and spawn the drain loop. Idempotent."""
        if self._task is not None and not self._task.done():
            return
        self._sub = self._bus.subscribe(consumer_id=CONSUMER_ID, max_queue=self._max_queue)
        self._task = asyncio.create_task(self._drain(), name="snapshot-drain")

    async def stop(self) -> None:
        """Cancel the drain loop and close the subscription. Idempotent.

        Snapshot data is preserved across stop / start cycles — the store is a
        cache, not a session.
        """
        if self._sub is not None and not self._sub.closed:
            self._sub.close()
        if self._task is not None:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task
            self._task = None
        self._sub = None

    # ------------------------------------------------------------------
    # Topic tracking — wired into registry callbacks at startup
    # ------------------------------------------------------------------

    async def track(self, topic: str) -> None:
        """Begin observing ``topic``. Idempotent. No-op until ``start()``
        has been awaited (the subscription is allocated there)."""
        if self._sub is None or self._sub.closed:
            return
        self._sub.add_topic(topic)

    async def untrack(self, topic: str) -> None:
        """Stop observing ``topic``. Cached snapshots are NOT cleared — last
        known state remains queryable while the topic is idle."""
        if self._sub is None or self._sub.closed:
            return
        self._sub.remove_topic(topic)

    # ------------------------------------------------------------------
    # Inspection
    # ------------------------------------------------------------------

    def get(self, product_id: str) -> SymbolSnapshot | None:
        return self._snapshots.get(product_id)

    def all(self) -> list[SymbolSnapshot]:
        return list(self._snapshots.values())

    def product_ids(self) -> frozenset[str]:
        return frozenset(self._snapshots.keys())

    # ------------------------------------------------------------------
    # Drain loop
    # ------------------------------------------------------------------

    async def _drain(self) -> None:
        assert self._sub is not None  # guaranteed by start() ordering
        # async-for terminates cleanly on subscription close; cancellation
        # propagates out so stop() can join us.
        async for msg in self._sub:
            try:
                self._apply(msg)
            except Exception as exc:  # noqa: BLE001 - one bad msg must not kill the loop
                self._log.error(
                    "snapshot.apply_error",
                    error_type=type(exc).__name__,
                    error=str(exc),
                    topic=msg.topic,
                )

    def _apply(self, msg: Message) -> None:
        payload = msg.payload
        snap = self._snapshots.setdefault(
            payload.product_id, SymbolSnapshot(product_id=payload.product_id)
        )
        snap.last_update_at = msg.received_at
        if isinstance(payload, Ticker):
            snap.last_ticker = payload
            snap.best_bid = payload.best_bid
            snap.best_bid_size = payload.best_bid_size
            snap.best_ask = payload.best_ask
            snap.best_ask_size = payload.best_ask_size
        elif isinstance(payload, Trade):
            snap.last_trade = payload
        elif isinstance(payload, L2Snapshot):
            # Coinbase emits bids descending and asks ascending, but our model
            # doesn't enforce ordering — sort defensively so the top-of-book
            # is correct regardless of upstream behaviour.
            if payload.bids:
                top = max(payload.bids, key=lambda b: b[0])
                snap.best_bid = top[0]
                snap.best_bid_size = top[1]
            if payload.asks:
                top = min(payload.asks, key=lambda a: a[0])
                snap.best_ask = top[0]
                snap.best_ask_size = top[1]
        elif isinstance(payload, L2Update):
            # Incremental updates: timestamp only — book reconstruction is out
            # of scope (see module docstring). Top-of-book stays at the last
            # value we got from a Ticker or L2Snapshot.
            pass
