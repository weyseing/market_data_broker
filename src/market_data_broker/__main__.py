"""Entry point for ``python -m market_data_broker``.

Wires bus + registry + Coinbase ingest. The downstream WS server (Step 7) and
MCP server (Step 8) are not yet present, so this module also supports an
operator-driven debug consumer for manual verification of the ingest path.

Manual smoke test against real Coinbase::

    MDB_DEBUG_SUBSCRIBE=coinbase.ticker.BTC-USD python -m market_data_broker

Set ``MDB_DEBUG_SUBSCRIBE`` to a comma-separated list of topics to register a
logging consumer that prints each received message at INFO level. Ctrl+C exits
cleanly (registry tears down, ingest closes upstream WS).
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


async def _debug_consumer(registry: Registry, topics: list[str]) -> None:
    """Register a consumer that logs every message it receives. Used for
    manual end-to-end verification before the WS / MCP servers exist."""
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
    registry = Registry(
        bus,
        on_first_subscriber=ingest.on_first_subscriber,
        on_last_unsubscriber=ingest.on_last_unsubscriber,
    )
    ingest.attach_registry(registry)

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)

    await ingest.start()

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
        await ingest.stop()
        log.info("hub stopped")


def main() -> None:
    configure_logging(level=os.environ.get("LOG_LEVEL", "INFO"))
    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
