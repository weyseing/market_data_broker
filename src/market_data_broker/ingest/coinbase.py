"""Coinbase WebSocket ingestion.

Owns a single WS connection to the Coinbase Exchange public feed and translates
demand from the registry (downstream subscribers) into upstream subscribe /
unsubscribe frames. Parsed data frames are published as :class:`Message`
envelopes to the in-memory bus.

Wiring
------

>>> bus = InMemoryBus()
>>> ingest = CoinbaseIngest(bus=bus, registry=None, ws_url="wss://...")
>>> registry = Registry(
...     bus,
...     on_first_subscriber=ingest.on_first_subscriber,
...     on_last_unsubscriber=ingest.on_last_unsubscriber,
... )
>>> ingest.attach_registry(registry)   # registry built second; back-fill the link
>>> await ingest.start()

Demand-driven model
-------------------

The registry calls :meth:`on_first_subscriber` at every ``0 → 1`` topic
transition and :meth:`on_last_unsubscriber` at every ``1 → 0``. The callbacks
just mutate ``_desired`` and signal the reconcile loop — they never block on
the network. The reconcile loop diffs ``_desired`` vs ``_active_upstream`` and
sends sub/unsub frames grouped by channel.

On (re)connect we clear ``_active_upstream`` (the server has zero subs after a
fresh handshake) and signal reconcile, which causes the full live set to be
re-subscribed in one batch per channel — the standard Coinbase reconnect
recipe.

Health
------

Two failure modes drive a reconnect:

1. **TCP / WS-level:** the underlying ``websockets`` connection raises (close
   code, OS error, timeout). We catch these in the run loop, mark the upstream
   ``reconnecting``, sleep with exponential backoff (capped), and reconnect.
2. **Stale feed:** application-level — if no frame arrives for
   ``heartbeat_timeout_seconds`` we forcibly close the WS, which surfaces as
   case (1). The library's ``ping_interval`` / ``ping_timeout`` only catch dead
   TCP; an idle-but-alive connection is what this watchdog covers.

Frame filtering
---------------

``subscriptions`` / ``heartbeat`` / ``error`` frames are non-data: filtered
before :func:`parse_coinbase_frame`. ``error`` frames are logged at WARNING.
Validation errors on data frames are logged and dropped — one bad frame must
not kill the session.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import time
from collections.abc import AsyncIterator, Awaitable, Callable
from typing import TYPE_CHECKING, Any, Protocol

from pydantic import ValidationError

from ..logging_config import get_logger
from ..models import UnknownFrameTypeError, parse_coinbase_frame
from ..topics import (
    COINBASE_CHANNELS_IN_SCOPE,
    VENUE_COINBASE,
    InvalidTopicError,
    parse_topic,
)

if TYPE_CHECKING:
    from ..bus import InMemoryBus
    from ..registry import Registry

DEFAULT_WS_URL = "wss://ws-feed.exchange.coinbase.com"
DEFAULT_INITIAL_BACKOFF = 1.0
DEFAULT_MAX_BACKOFF = 30.0
DEFAULT_HEARTBEAT_TIMEOUT = 15.0
# Frames bigger than the websockets default 1 MiB are real (level2 snapshots
# are ~1 MB). Cap at 4 MiB — generous, but bounded.
DEFAULT_MAX_FRAME_BYTES = 4 * 1024 * 1024

# Non-data control frames the receive loop must skip without parsing.
_CONTROL_FRAME_TYPES: frozenset[str] = frozenset({"subscriptions", "heartbeat", "error"})


class WebSocketLike(Protocol):
    """Subset of ``websockets.ClientConnection`` we depend on.

    Tests substitute a fake; production uses ``websockets.connect``.
    """

    async def send(self, data: str) -> None: ...

    async def close(self, *, code: int = 1000, reason: str = "") -> None: ...

    def __aiter__(self) -> AsyncIterator[str | bytes]: ...


# A Connector is anything that, when called, returns an async context manager
# yielding a :class:`WebSocketLike`. This matches ``websockets.connect(url, ...)``
# directly, and lets tests inject fakes without monkey-patching the module.
Connector = Callable[[], Any]


class CoinbaseIngest:
    """Coinbase WS ingest adapter.

    Construct, wire its callbacks into the registry, then ``await start()``.
    The adapter owns a single background task; ``await stop()`` cancels it
    cleanly and tears down the WS connection.
    """

    UPSTREAM_NAME = "coinbase"

    def __init__(
        self,
        *,
        bus: InMemoryBus,
        connector: Connector | None = None,
        ws_url: str = DEFAULT_WS_URL,
        initial_backoff_seconds: float = DEFAULT_INITIAL_BACKOFF,
        max_backoff_seconds: float = DEFAULT_MAX_BACKOFF,
        heartbeat_timeout_seconds: float = DEFAULT_HEARTBEAT_TIMEOUT,
        max_frame_bytes: int | None = DEFAULT_MAX_FRAME_BYTES,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        self._bus = bus
        self._registry: Registry | None = None
        self._ws_url = ws_url
        self._initial_backoff = initial_backoff_seconds
        self._max_backoff = max_backoff_seconds
        self._heartbeat_timeout = heartbeat_timeout_seconds
        self._max_frame_bytes = max_frame_bytes
        self._sleep = sleep
        self._monotonic = monotonic
        self._log = get_logger("ingest.coinbase")

        # Lazy: production code defaults to websockets.connect; tests inject
        # their own. We don't import websockets at module top-level so that
        # tests can run without it installed in odd environments.
        self._connector: Connector | None = connector

        # (channel, product_id) tuples. Source of truth for what the hub *wants*
        # to be subscribed to upstream. Mutated by callbacks.
        self._desired: set[tuple[str, str]] = set()
        # What we believe is currently subscribed on the wire. Cleared on every
        # (re)connect because a fresh handshake starts with zero subs.
        self._active_upstream: set[tuple[str, str]] = set()
        self._reconcile_event = asyncio.Event()

        self._runner: asyncio.Task[None] | None = None
        self._stopping = asyncio.Event()
        self._ws: WebSocketLike | None = None
        self._last_msg_at: float = 0.0

    # ------------------------------------------------------------------
    # Wiring
    # ------------------------------------------------------------------

    def attach_registry(self, registry: Registry) -> None:
        """Late-bind the registry. Needed because :class:`Registry` takes the
        callbacks at construction, so the natural creation order is
        ``ingest → registry(callbacks=ingest.on_*) → ingest.attach_registry(registry)``.
        Without the registry we still publish, but message-rate accounting
        (``record_message``) is skipped."""
        self._registry = registry

    # ------------------------------------------------------------------
    # Registry callbacks (wired into Registry on_first / on_last)
    # ------------------------------------------------------------------

    async def on_first_subscriber(self, topic: str) -> None:
        """Topic transitioned 0 → 1 holders. Add to desired set + signal."""
        parsed = self._parse_coinbase_topic(topic)
        if parsed is None:
            return
        self._desired.add(parsed)
        self._reconcile_event.set()

    async def on_last_unsubscriber(self, topic: str) -> None:
        """Topic transitioned 1 → 0 holders. Remove from desired set + signal."""
        parsed = self._parse_coinbase_topic(topic)
        if parsed is None:
            return
        self._desired.discard(parsed)
        self._reconcile_event.set()

    def _parse_coinbase_topic(self, topic: str) -> tuple[str, str] | None:
        """Return ``(channel, product_id)`` if ``topic`` belongs to this venue
        and channel is in scope; otherwise log and return ``None``.

        Non-coinbase topics are silently ignored — the registry is venue-agnostic
        so a future binance ingest will see the same callback fire for every
        topic; each adapter filters by venue.
        """
        try:
            parts = parse_topic(topic)
        except InvalidTopicError:
            self._log.warning("ingest.skip_invalid_topic", topic=topic)
            return None
        if parts.venue != VENUE_COINBASE:
            return None
        if parts.channel not in COINBASE_CHANNELS_IN_SCOPE:
            self._log.warning(
                "ingest.skip_unsupported_channel",
                topic=topic,
                channel=parts.channel,
                supported=sorted(COINBASE_CHANNELS_IN_SCOPE),
            )
            return None
        return (parts.channel, parts.product_id)

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def start(self) -> None:
        """Spawn the background run loop. Idempotent."""
        if self._runner is not None and not self._runner.done():
            return
        self._stopping.clear()
        self._runner = asyncio.create_task(self._run(), name="coinbase-ingest")

    async def stop(self) -> None:
        """Signal the run loop to exit, close WS, await teardown. Idempotent."""
        self._stopping.set()
        # Wake any sleeping reconcile loop / backoff sleep so we don't have to
        # wait for the timer to elapse.
        self._reconcile_event.set()
        if self._ws is not None:
            with contextlib.suppress(Exception):
                await self._ws.close()
        if self._runner is not None:
            self._runner.cancel()
            try:
                await self._runner
            except asyncio.CancelledError:
                pass
            except Exception as exc:  # noqa: BLE001 - teardown must not raise
                # The run loop already logged the cause; surface at debug so
                # repeated stop() calls during teardown don't bubble errors.
                self._log.debug(
                    "ingest.runner_exited_with_error",
                    error_type=type(exc).__name__,
                    error=str(exc),
                )
            self._runner = None
        if self._registry is not None:
            upstream = self._registry.upstream(self.UPSTREAM_NAME)
            if upstream is not None:
                upstream.mark_disconnected()

    # ------------------------------------------------------------------
    # Run loop — connect / handle session / backoff / reconnect
    # ------------------------------------------------------------------

    async def _run(self) -> None:
        if self._connector is None:
            self._connector = self._default_connector()
        upstream = self._ensure_upstream_registered()
        backoff = self._initial_backoff
        while not self._stopping.is_set():
            try:
                async with self._connector() as ws:
                    self._ws = ws
                    upstream.mark_connected()
                    self._log.info("ingest.connected", url=self._ws_url)
                    backoff = self._initial_backoff
                    # Server has zero subs on a fresh handshake; we need to
                    # re-subscribe everything in `_desired`.
                    self._active_upstream.clear()
                    self._last_msg_at = self._monotonic()
                    self._reconcile_event.set()
                    await self._handle_session(ws)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self._log.warning(
                    "ingest.session_error",
                    error_type=type(exc).__name__,
                    error=str(exc),
                )
            finally:
                self._ws = None
            if self._stopping.is_set():
                break
            upstream.mark_reconnecting()
            self._log.info("ingest.backoff_sleep", seconds=backoff)
            try:
                await self._sleep(backoff)
            except asyncio.CancelledError:
                raise
            backoff = min(backoff * 2, self._max_backoff)
        upstream.mark_disconnected()
        self._log.info("ingest.stopped")

    async def _handle_session(self, ws: WebSocketLike) -> None:
        """Run receive + reconcile + watchdog concurrently. First one to
        finish (cleanly or with error) cancels the others."""
        receive = asyncio.create_task(self._receive_loop(ws), name="coinbase-receive")
        reconcile = asyncio.create_task(self._reconcile_loop(ws), name="coinbase-reconcile")
        watchdog = asyncio.create_task(self._watchdog_loop(ws), name="coinbase-watchdog")
        tasks = {receive, reconcile, watchdog}
        try:
            done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
        finally:
            for t in tasks:
                if not t.done():
                    t.cancel()
            for t in tasks:
                with contextlib.suppress(asyncio.CancelledError, Exception):
                    await t
        # Re-raise any exception from the first-to-finish so the run loop can
        # see it and decide whether to reconnect.
        for t in done:
            exc = t.exception()
            if exc is not None:
                raise exc

    # ------------------------------------------------------------------
    # Receive loop
    # ------------------------------------------------------------------

    async def _receive_loop(self, ws: WebSocketLike) -> None:
        async for raw in ws:
            self._last_msg_at = self._monotonic()
            try:
                data = json.loads(raw)
            except json.JSONDecodeError:
                self._log.warning("ingest.bad_json", sample=str(raw)[:200])
                continue
            if not isinstance(data, dict):
                self._log.warning("ingest.non_object_frame", sample=str(data)[:200])
                continue
            frame_type = data.get("type")
            if frame_type in _CONTROL_FRAME_TYPES:
                if frame_type == "error":
                    self._log.warning("ingest.coinbase_error_frame", frame=data)
                continue
            try:
                message = parse_coinbase_frame(data)
            except UnknownFrameTypeError:
                self._log.warning("ingest.unknown_frame_type", frame_type=frame_type)
                continue
            except (ValidationError, KeyError) as exc:
                self._log.warning(
                    "ingest.frame_validation_failed",
                    frame_type=frame_type,
                    error=str(exc),
                )
                continue
            await self._bus.publish(message)
            if self._registry is not None:
                self._registry.record_message(message.topic)

    # ------------------------------------------------------------------
    # Reconcile loop — push desired state to the wire
    # ------------------------------------------------------------------

    async def _reconcile_loop(self, ws: WebSocketLike) -> None:
        upstream = self._ensure_upstream_registered()
        while True:
            await self._reconcile_event.wait()
            self._reconcile_event.clear()
            if self._stopping.is_set():
                return
            to_add = self._desired - self._active_upstream
            to_remove = self._active_upstream - self._desired
            if to_add:
                for channel, product_ids in self._group_by_channel(to_add).items():
                    await ws.send(
                        json.dumps(
                            {
                                "type": "subscribe",
                                "product_ids": product_ids,
                                "channels": [channel],
                            }
                        )
                    )
                    self._log.info(
                        "ingest.subscribe_sent",
                        channel=channel,
                        product_ids=product_ids,
                    )
                    for pid in product_ids:
                        self._active_upstream.add((channel, pid))
            if to_remove:
                for channel, product_ids in self._group_by_channel(to_remove).items():
                    await ws.send(
                        json.dumps(
                            {
                                "type": "unsubscribe",
                                "product_ids": product_ids,
                                "channels": [channel],
                            }
                        )
                    )
                    self._log.info(
                        "ingest.unsubscribe_sent",
                        channel=channel,
                        product_ids=product_ids,
                    )
                    for pid in product_ids:
                        self._active_upstream.discard((channel, pid))
            upstream.topics = {
                f"{VENUE_COINBASE}.{ch}.{pid}" for ch, pid in self._active_upstream
            }

    @staticmethod
    def _group_by_channel(pairs: set[tuple[str, str]]) -> dict[str, list[str]]:
        """Group ``(channel, product_id)`` pairs into ``{channel: [product_id, ...]}``.

        Sorted product lists make the wire frames deterministic — helpful for
        tests and for log readability."""
        out: dict[str, list[str]] = {}
        for channel, product_id in pairs:
            out.setdefault(channel, []).append(product_id)
        for channel in out:
            out[channel].sort()
        return out

    # ------------------------------------------------------------------
    # Watchdog — force reconnect on stale feed
    # ------------------------------------------------------------------

    async def _watchdog_loop(self, ws: WebSocketLike) -> None:
        # Poll at ~1 Hz (or faster than the timeout, whichever is smaller) so
        # tests with sub-second timeouts respond quickly.
        check_interval = max(min(self._heartbeat_timeout / 4, 1.0), 0.01)
        while True:
            await self._sleep(check_interval)
            if self._stopping.is_set():
                return
            # Only enforce after we've seen at least one frame OR we've been
            # actively expecting frames (i.e. desired is non-empty). Otherwise
            # an idle hub on startup with no subscribers would self-reconnect.
            if not self._desired:
                continue
            elapsed = self._monotonic() - self._last_msg_at
            if elapsed > self._heartbeat_timeout:
                self._log.warning(
                    "ingest.stale_feed_reconnect",
                    elapsed_seconds=round(elapsed, 3),
                    timeout_seconds=self._heartbeat_timeout,
                )
                with contextlib.suppress(Exception):
                    await ws.close(code=1011, reason="stale feed")
                return

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _ensure_upstream_registered(self):  # type: ignore[no-untyped-def]
        """Idempotently register this venue with the registry. Returns the
        :class:`UpstreamConnection` so callers can mutate state."""
        if self._registry is None:
            raise RuntimeError(
                "CoinbaseIngest.attach_registry() must be called before start(); "
                "the registry is needed for upstream state tracking"
            )
        existing = self._registry.upstream(self.UPSTREAM_NAME)
        if existing is not None:
            return existing
        return self._registry.register_upstream(self.UPSTREAM_NAME)

    def _default_connector(self) -> Connector:
        # Imported lazily so unit tests don't have to load websockets just to
        # exercise the run loop with a fake connector.
        import websockets

        url = self._ws_url
        max_size = self._max_frame_bytes

        def connector():  # type: ignore[no-untyped-def]
            return websockets.connect(
                url,
                ping_interval=20,
                ping_timeout=20,
                max_size=max_size,
            )

        return connector

    # ------------------------------------------------------------------
    # Inspection (handy for tests + future /status)
    # ------------------------------------------------------------------

    @property
    def desired_topics(self) -> frozenset[tuple[str, str]]:
        return frozenset(self._desired)

    @property
    def active_upstream_topics(self) -> frozenset[tuple[str, str]]:
        return frozenset(self._active_upstream)
