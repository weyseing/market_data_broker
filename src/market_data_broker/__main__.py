"""Entry point for ``python -m market_data_broker``.

Wires bus + registry + Coinbase ingest + downstream WebSocket server. The MCP
server (Step 8) is still pending, so this module also supports an
operator-driven debug consumer for manual verification of the ingest path
without needing a WS client.

Manual smoke tests against real Coinbase::

    # Logs every message at INFO directly from a registered consumer.
    MDB_DEBUG_SUBSCRIBE=coinbase.ticker.BTC-USD python -m market_data_broker

    # Connect a WS client (e.g. websocat) and drive subscriptions over the wire.
    python -m market_data_broker
    websocat ws://127.0.0.1:8765
    > {"action":"subscribe","topics":["coinbase.ticker.BTC-USD"]}

Both routes share the same registry, so refcounts compose: ingest only holds
the upstream subscription while at least one of {WS clients, debug consumer}
still wants the topic. Ctrl+C exits cleanly.
"""

from __future__ import annotations

import asyncio
import contextlib
import os
import signal

from market_data_broker.bus import InMemoryBus
from market_data_broker.ingest.coinbase import CoinbaseIngest
from market_data_broker.logging_config import configure_logging, get_logger
from market_data_broker.registry import Registry
from market_data_broker.server import DownstreamWSServer
from market_data_broker.snapshot import SnapshotStore

DEFAULT_WS_HOST = "0.0.0.0"
DEFAULT_WS_PORT = 8765


async def _debug_consumer(registry: Registry, topics: list[str]) -> None:
    """Register a consumer that logs every message it receives. Used for
    manual end-to-end verification without needing a WS client."""
    log = get_logger("debug_consumer")
    sub = await registry.register_consumer("debug-cli", topics)
    log.info("debug_consumer.subscribed", topics=topics)
    try:
        async for msg in sub:
            log.info(
                "debug_consumer.msg",
                topic=msg.topic,
                payload_type=msg.payload.type,
                payload=msg.payload.model_dump(mode="json"),
            )
    finally:
        await registry.unregister_consumer("debug-cli")


async def run() -> None:
    log = get_logger("hub")
    log.info("hub starting", version="0.1.0")

    bus = InMemoryBus()
    ingest = CoinbaseIngest(bus=bus)
    snapshot = SnapshotStore(bus)

    # Compose: each topic transition fans out to both the ingest adapter
    # (drives upstream sub/unsub) and the snapshot store (starts/stops
    # observing the topic). Snapshot is a passive observer — it does NOT go
    # through the registry, so it never pins a topic open against demand.
    async def on_first(topic: str) -> None:
        await ingest.on_first_subscriber(topic)
        await snapshot.track(topic)

    async def on_last(topic: str) -> None:
        await ingest.on_last_unsubscriber(topic)
        await snapshot.untrack(topic)

    registry = Registry(
        bus,
        on_first_subscriber=on_first,
        on_last_unsubscriber=on_last,
    )
    ingest.attach_registry(registry)

    ws_server = DownstreamWSServer(
        registry,
        host=os.environ.get("MDB_WS_HOST", DEFAULT_WS_HOST),
        port=int(os.environ.get("MDB_WS_PORT", DEFAULT_WS_PORT)),
    )

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)

    # Start snapshot before ingest so the bus subscription is ready by the
    # time the first frames flow. WS server starts last — it's the public
    # surface, so we want the inner layers ready before we accept clients.
    await snapshot.start()
    await ingest.start()
    await ws_server.start()

    debug_topics_raw = os.environ.get("MDB_DEBUG_SUBSCRIBE", "").strip()
    debug_task: asyncio.Task[None] | None = None
    if debug_topics_raw:
        topics = [t.strip() for t in debug_topics_raw.split(",") if t.strip()]
        debug_task = asyncio.create_task(_debug_consumer(registry, topics))

    log.info("hub ready")
    try:
        await stop.wait()
    finally:
        log.info("hub stopping")
        if debug_task is not None:
            debug_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await debug_task
        # Tear down outermost first: stop accepting / disconnect WS clients,
        # then ingest, then drain snapshot. Each layer's stop is idempotent
        # so partial-startup teardown is safe too.
        await ws_server.stop()
        await ingest.stop()
        await snapshot.stop()
        log.info("hub stopped")


def main() -> None:
    configure_logging(level=os.environ.get("LOG_LEVEL", "INFO"))
    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
