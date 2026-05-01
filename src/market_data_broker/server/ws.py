"""Downstream WebSocket server.

A protocol-thin streaming endpoint for non-MCP consumers: language-agnostic
clients (``websocat``, browser code, internal services) open a WebSocket,
subscribe to topics, and receive each :class:`~market_data_broker.models.Message`
fan-out as it lands on the bus.

Wire protocol
-------------

Every frame is a single JSON object.

**Client → server**::

    {"action": "subscribe",   "topics": ["coinbase.ticker.BTC-USD", ...]}
    {"action": "unsubscribe", "topics": ["coinbase.ticker.BTC-USD", ...]}

**Server → client**::

    {"kind": "welcome", "consumer_id": "ws-7"}
    {"kind": "ack",     "action": "subscribe",   "topics": [...]}
    {"kind": "ack",     "action": "unsubscribe", "topics": [...]}
    {"kind": "error",   "code": "...", "message": "...", "topic": "..."}
    {"kind": "data",    "topic": "...", "venue": "...", "received_at": "...",
                        "payload": {...}}

The protocol is intentionally minimal — topic discovery, schema documentation,
and snapshot queries belong to the MCP surface (Step 8). Clients of this
endpoint are expected to know the topics they want from the docs.

Why each WS connection is a registry consumer
---------------------------------------------

The registry is the single source of truth for subscriptions: ref-counts drive
upstream (un)subscribe and the ``/status`` view. Going through the registry
keeps WS clients indistinguishable from any other consumer (snapshot store
aside, which is intentionally bus-direct), so a leaked WS connection cannot
leak an upstream subscription — when the WS closes for any reason the
``finally`` in the handler unregisters the consumer, which decrements every
ref-count it held.

Backpressure
------------

The bus drops the *oldest* queued message when a consumer's queue is full. A
slow client therefore cannot stall publishers or peers — but can still receive
stale data forever. ``max_dropped_messages`` (cumulative, per-connection)
caps that: once exceeded, the server sends a ``backpressure_exceeded`` error
and closes the connection with code 1013 (try again later). ``None`` disables
the cap (drop-oldest only).
"""

from __future__ import annotations

import asyncio
import contextlib
import itertools
import json
from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING, Any

from websockets.asyncio.server import ServerConnection, serve
from websockets.exceptions import ConnectionClosed

from ..bus import Subscription
from ..logging_config import get_logger
from ..topics import InvalidTopicError, parse_topic

if TYPE_CHECKING:
    from ..registry import Registry

DEFAULT_HOST = "0.0.0.0"
DEFAULT_PORT = 8765
# Generous per-frame cap: client frames are tiny subscribe envelopes, but lift
# the websockets default 1 MiB enough that a misbehaving client fails fast
# rather than getting silent close-1009s.
DEFAULT_MAX_FRAME_BYTES = 64 * 1024
# Mirrors config/default.yaml: bus.sustained_overflow_drops.
DEFAULT_MAX_DROPPED_MESSAGES = 100

# Close codes used when the server initiates a disconnect.
_CLOSE_BACKPRESSURE = 1013  # "try again later"
_CLOSE_NORMAL = 1000

_consumer_counter = itertools.count(1)


# A factory that returns a Server-like awaitable; injectable so tests don't
# need to bind a real socket. Defaults to ``websockets.asyncio.server.serve``.
ServeFn = Callable[..., Awaitable[Any]]


class DownstreamWSServer:
    """WebSocket fan-out server.

    Lifecycle::

        server = DownstreamWSServer(registry)
        await server.start()
        ...
        await server.stop()

    ``start`` is idempotent; ``stop`` closes the listening socket *and* every
    in-flight client connection (handler ``finally`` blocks unregister consumers
    so no upstream subscriptions leak).
    """

    def __init__(
        self,
        registry: Registry,
        *,
        host: str = DEFAULT_HOST,
        port: int = DEFAULT_PORT,
        max_queue: int | None = None,
        max_dropped_messages: int | None = DEFAULT_MAX_DROPPED_MESSAGES,
        max_frame_bytes: int = DEFAULT_MAX_FRAME_BYTES,
        serve_fn: ServeFn | None = None,
    ) -> None:
        self._registry = registry
        self._host = host
        self._port = port
        self._max_queue = max_queue
        self._max_dropped_messages = max_dropped_messages
        self._max_frame_bytes = max_frame_bytes
        self._serve_fn: ServeFn = serve_fn or serve
        self._server: Any = None
        self._log = get_logger("server.ws")

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def start(self) -> None:
        """Bind the listening socket and start accepting connections."""
        if self._server is not None:
            return
        self._server = await self._serve_fn(
            self._handler,
            self._host,
            self._port,
            max_size=self._max_frame_bytes,
        )
        self._log.info("ws_server.started", host=self._host, port=self.port)

    async def stop(self) -> None:
        """Close the listener and tear down all in-flight connections.

        ``Server.close()`` cancels the active handler tasks; their ``finally``
        blocks unregister each consumer from the registry, which fires
        ``on_last_unsubscriber`` for any topic this client was the last holder
        of. ``wait_closed`` then blocks until cleanup completes."""
        if self._server is None:
            return
        self._server.close()
        await self._server.wait_closed()
        self._server = None
        self._log.info("ws_server.stopped")

    @property
    def port(self) -> int:
        """Actual bound port. Useful when constructed with ``port=0`` (tests)."""
        if self._server is None:
            return self._port
        sockets = self._server.sockets
        if not sockets:
            return self._port
        return sockets[0].getsockname()[1]

    # ------------------------------------------------------------------
    # Per-connection handler
    # ------------------------------------------------------------------

    async def _handler(self, ws: ServerConnection) -> None:
        consumer_id = f"ws-{next(_consumer_counter)}"
        log = self._log.bind(consumer_id=consumer_id, peer=_peer_str(ws))
        log.info("ws_server.client_connected")

        sub: Subscription | None = None
        try:
            sub = await self._registry.register_consumer(
                consumer_id, [], max_queue=self._max_queue
            )
        except Exception as exc:  # noqa: BLE001 - any registration failure → reject cleanly
            log.warning("ws_server.register_failed", error=str(exc))
            with contextlib.suppress(Exception):
                await _send(ws, {"kind": "error", "code": "register_failed",
                                 "message": str(exc)})
                await ws.close(code=1011, reason="register failed")
            return

        try:
            await _send(ws, {"kind": "welcome", "consumer_id": consumer_id})
            recv_task = asyncio.create_task(
                self._recv_loop(ws, consumer_id, log), name=f"{consumer_id}-recv"
            )
            forward_task = asyncio.create_task(
                self._forward_loop(ws, sub, consumer_id, log),
                name=f"{consumer_id}-forward",
            )
            tasks = {recv_task, forward_task}
            try:
                done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
            finally:
                for t in tasks:
                    if not t.done():
                        t.cancel()
                for t in tasks:
                    with contextlib.suppress(asyncio.CancelledError, Exception):
                        await t
            for t in done:
                exc = t.exception()
                if exc is not None and not isinstance(exc, ConnectionClosed):
                    log.warning(
                        "ws_server.task_error",
                        task=t.get_name(),
                        error_type=type(exc).__name__,
                        error=str(exc),
                    )
        finally:
            # Unregister fires on_last_unsubscriber for every topic this client
            # was the last holder of — the no-leaks guarantee.
            with contextlib.suppress(Exception):
                await self._registry.unregister_consumer(consumer_id)
            log.info("ws_server.client_disconnected")

    # ------------------------------------------------------------------
    # Receive loop — translate client frames into registry calls
    # ------------------------------------------------------------------

    async def _recv_loop(
        self, ws: ServerConnection, consumer_id: str, log: Any
    ) -> None:
        try:
            async for raw in ws:
                await self._handle_frame(ws, consumer_id, raw, log)
        except ConnectionClosed:
            return

    async def _handle_frame(
        self,
        ws: ServerConnection,
        consumer_id: str,
        raw: str | bytes,
        log: Any,
    ) -> None:
        if isinstance(raw, bytes):
            try:
                raw = raw.decode("utf-8")
            except UnicodeDecodeError:
                await _send_error(ws, "bad_json", "frame is not valid UTF-8")
                return
        try:
            data = json.loads(raw)
        except json.JSONDecodeError as exc:
            await _send_error(ws, "bad_json", f"frame is not valid JSON: {exc.msg}")
            return
        if not isinstance(data, dict):
            await _send_error(ws, "bad_request", "frame must be a JSON object")
            return

        action = data.get("action")
        if action not in ("subscribe", "unsubscribe"):
            await _send_error(
                ws,
                "bad_request",
                f"action must be 'subscribe' or 'unsubscribe', got {action!r}",
            )
            return

        raw_topics = data.get("topics")
        if not isinstance(raw_topics, list) or not all(
            isinstance(t, str) for t in raw_topics
        ):
            await _send_error(ws, "bad_request", "topics must be a list of strings")
            return

        # Validate topics individually so one bad entry doesn't reject the whole
        # batch — clients can fix the typo and re-send only that topic.
        ok_topics: list[str] = []
        for topic in raw_topics:
            try:
                parse_topic(topic)
            except InvalidTopicError as exc:
                await _send_error(ws, "invalid_topic", str(exc), topic=topic)
                continue
            ok_topics.append(topic)

        applied: list[str] = []
        for topic in ok_topics:
            try:
                if action == "subscribe":
                    await self._registry.add_topic(consumer_id, topic)
                else:
                    await self._registry.remove_topic(consumer_id, topic)
            except Exception as exc:  # noqa: BLE001 - keep client session alive
                log.warning(
                    "ws_server.action_failed",
                    action=action,
                    topic=topic,
                    error_type=type(exc).__name__,
                    error=str(exc),
                )
                await _send_error(
                    ws,
                    f"{action}_failed",
                    f"{action} for topic {topic} failed: {exc}",
                    topic=topic,
                )
                continue
            applied.append(topic)

        await _send(ws, {"kind": "ack", "action": action, "topics": applied})

    # ------------------------------------------------------------------
    # Forward loop — bus → wire
    # ------------------------------------------------------------------

    async def _forward_loop(
        self,
        ws: ServerConnection,
        sub: Subscription,
        consumer_id: str,
        log: Any,
    ) -> None:
        threshold = self._max_dropped_messages
        try:
            async for msg in sub:
                if threshold is not None and sub.dropped_messages > threshold:
                    log.warning(
                        "ws_server.backpressure_disconnect",
                        dropped=sub.dropped_messages,
                        threshold=threshold,
                    )
                    with contextlib.suppress(Exception):
                        await _send_error(
                            ws,
                            "backpressure_exceeded",
                            f"dropped {sub.dropped_messages} messages "
                            f"(> {threshold}); closing connection",
                        )
                        await ws.close(
                            code=_CLOSE_BACKPRESSURE, reason="backpressure"
                        )
                    return
                frame = msg.model_dump(mode="json")
                frame["kind"] = "data"
                await ws.send(json.dumps(frame))
        except ConnectionClosed:
            return


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


async def _send(ws: ServerConnection, payload: dict[str, Any]) -> None:
    await ws.send(json.dumps(payload))


async def _send_error(
    ws: ServerConnection,
    code: str,
    message: str,
    *,
    topic: str | None = None,
) -> None:
    frame: dict[str, Any] = {"kind": "error", "code": code, "message": message}
    if topic is not None:
        frame["topic"] = topic
    with contextlib.suppress(Exception):
        await ws.send(json.dumps(frame))


def _peer_str(ws: ServerConnection) -> str:
    """Best-effort ``host:port`` for logs — websockets drops this on close, so
    we capture it eagerly. Returns ``"?"`` if unavailable."""
    try:
        addr = ws.remote_address
    except Exception:  # noqa: BLE001 - logging only
        return "?"
    if not addr:
        return "?"
    return f"{addr[0]}:{addr[1]}"
