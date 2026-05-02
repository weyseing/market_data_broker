"""Entry point for ``python -m market_data_broker``.

Two run modes:

- **hub** (default) — bus + Coinbase ingest + downstream WebSocket + HTTP
  /status. The standard server deployment.
- **mcp** — bus + ingest + snapshot + an MCP server (stdio or streamable-http
  transport). Used by LLM agents (Claude Desktop, MCP inspector, remote agents).

Manual smoke tests against real Coinbase::

    # Hub mode (default).
    python -m market_data_broker

    # MCP over streamable-HTTP on port 8000 (default transport).
    python -m market_data_broker mcp

    # MCP over stdio — for parents that spawn the server (inspector,
    # Claude Desktop without the mcp-remote bridge).
    python -m market_data_broker mcp --stdio

    # Hub mode + a debug consumer for ingest verification without a WS client.
    MDB_DEBUG_SUBSCRIBE=coinbase.ticker.BTC-USD python -m market_data_broker

Both routes share the same registry, so refcounts compose: ingest only holds
the upstream subscription while at least one of {WS clients, MCP stream calls,
debug consumer} still wants the topic. Ctrl+C exits cleanly.

Runtime values (ports, URLs, timeouts, etc.) come from
:func:`market_data_broker.config.load_config`. See ``.env.example`` for the
full list of env-var overrides.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import os
import signal
import sys

from market_data_broker.bus import InMemoryBus
from market_data_broker.config import Config, ConfigError, load_config
from market_data_broker.ingest.coinbase import CoinbaseIngest
from market_data_broker.logging_config import configure_logging, get_logger
from market_data_broker.registry import Registry
from market_data_broker.server import (
    DownstreamWSServer,
    MarketDataMCPServer,
    StatusHTTPServer,
)
from market_data_broker.snapshot import SnapshotStore


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


def _build_core(cfg: Config) -> tuple[
    InMemoryBus, Registry, CoinbaseIngest, SnapshotStore
]:
    """Build the shared in-process core. Wiring is identical between hub and
    mcp modes; the difference is just which public surfaces get added on top."""
    bus = InMemoryBus(default_max_queue=cfg.bus.consumer_queue_size)
    ingest = CoinbaseIngest(
        bus=bus,
        ws_url=cfg.coinbase.ws_url,
        initial_backoff_seconds=cfg.coinbase.reconnect.initial_backoff_seconds,
        max_backoff_seconds=cfg.coinbase.reconnect.max_backoff_seconds,
        heartbeat_timeout_seconds=cfg.coinbase.heartbeat_timeout_seconds,
    )
    snapshot = SnapshotStore(bus)

    async def on_first(topic: str) -> None:
        await ingest.on_first_subscriber(topic)
        await snapshot.track(topic)

    async def on_last(topic: str) -> None:
        await ingest.on_last_unsubscriber(topic)
        await snapshot.untrack(topic)

    registry = Registry(bus, on_first_subscriber=on_first, on_last_unsubscriber=on_last)
    ingest.attach_registry(registry)
    return bus, registry, ingest, snapshot


def _install_stop_signal(stop: asyncio.Event) -> None:
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        with contextlib.suppress(NotImplementedError):
            loop.add_signal_handler(sig, stop.set)


async def run_hub(cfg: Config) -> None:
    """Hub mode — bus + ingest + WS server + HTTP /status. Runs until SIGINT."""
    log = get_logger("hub")
    log.info("hub.starting", version="0.1.0", mode="hub")

    bus, registry, ingest, snapshot = _build_core(cfg)
    ws_server = DownstreamWSServer(
        registry,
        host=cfg.ws_server.host,
        port=cfg.ws_server.port,
        max_dropped_messages=cfg.bus.sustained_overflow_drops,
    )
    status_server = StatusHTTPServer(
        registry,
        host=cfg.status_server.host,
        port=cfg.status_server.port,
    )

    stop = asyncio.Event()
    _install_stop_signal(stop)

    await snapshot.start()
    await ingest.start()
    await ws_server.start()
    await status_server.start()

    debug_topics_raw = os.environ.get("MDB_DEBUG_SUBSCRIBE", "").strip()
    debug_task: asyncio.Task[None] | None = None
    if debug_topics_raw:
        topics = [t.strip() for t in debug_topics_raw.split(",") if t.strip()]
        debug_task = asyncio.create_task(_debug_consumer(registry, topics))

    log.info("hub.ready")
    try:
        await stop.wait()
    finally:
        log.info("hub.stopping")
        if debug_task is not None:
            debug_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await debug_task
        await status_server.stop()
        await ws_server.stop()
        await ingest.stop()
        await snapshot.stop()
        log.info("hub.stopped")
    # Bus is in-memory only — nothing to close.
    del bus


async def run_mcp(cfg: Config, *, transport: str, host: str, port: int) -> None:
    """MCP mode — bus + ingest + snapshot + an MCP server.

    Stdio transport returns when the client (Claude Desktop / inspector)
    disconnects; HTTP transport runs until SIGINT. Either way, the surrounding
    ``finally`` tears down ingest + snapshot cleanly.
    """
    log = get_logger("hub")
    log.info("hub.starting", version="0.1.0", mode=f"mcp.{transport}")

    _, registry, ingest, snapshot = _build_core(cfg)
    mcp_server = MarketDataMCPServer(
        registry=registry, snapshot=snapshot, host=host, port=port
    )

    await snapshot.start()
    await ingest.start()
    log.info("hub.ready")

    runner: asyncio.Task[None]
    if transport == "stdio":
        runner = asyncio.create_task(mcp_server.run_stdio(), name="mcp-stdio")
    elif transport == "http":
        runner = asyncio.create_task(mcp_server.run_http(), name="mcp-http")
    else:
        raise ValueError(f"unknown transport {transport!r}")

    stop = asyncio.Event()
    _install_stop_signal(stop)

    async def _wait_for_stop() -> None:
        await stop.wait()

    waiter = asyncio.create_task(_wait_for_stop(), name="mcp-stop-waiter")
    try:
        await asyncio.wait({runner, waiter}, return_when=asyncio.FIRST_COMPLETED)
    finally:
        log.info("hub.stopping")
        runner.cancel()
        waiter.cancel()
        for t in (runner, waiter):
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await t
        await ingest.stop()
        await snapshot.stop()
        log.info("hub.stopped")


def _parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="market-data-broker",
        description="Real-time crypto market-data hub with MCP interface.",
    )
    sub = parser.add_subparsers(dest="command", metavar="{hub,mcp}")

    sub.add_parser("hub", help="Run the hub (WS + /status). Default if no command given.")

    mcp = sub.add_parser("mcp", help="Run the MCP server.")
    transport = mcp.add_mutually_exclusive_group()
    transport.add_argument(
        "--http",
        dest="transport",
        action="store_const",
        const="http",
        help=(
            "Speak MCP over streamable-HTTP (default — for Docker, remote "
            "agents, the inspector with Streamable-HTTP, and Claude Desktop "
            "via the mcp-remote bridge)."
        ),
    )
    transport.add_argument(
        "--stdio",
        dest="transport",
        action="store_const",
        const="stdio",
        help=(
            "Speak MCP over stdin/stdout (for parents that spawn the server "
            "directly — e.g. the inspector script, or Claude Desktop without "
            "the mcp-remote bridge)."
        ),
    )
    mcp.add_argument(
        "--host",
        default="127.0.0.1",
        help="HTTP bind host (only with --http). Default: 127.0.0.1.",
    )
    mcp.add_argument(
        "--port",
        type=int,
        default=8000,
        help="HTTP bind port (only with --http). Default: 8000.",
    )
    mcp.set_defaults(transport="http")
    return parser.parse_args(argv)


def main() -> None:
    args = _parse_args(sys.argv[1:])
    try:
        cfg = load_config()
    except ConfigError as exc:
        sys.stderr.write(f"config error: {exc}\n")
        sys.exit(2)

    # MCP stdio owns stdout for JSON-RPC framing — logs must go to stderr or
    # they will corrupt the protocol channel. Hub mode and MCP-over-HTTP keep
    # the existing stdout default.
    log_stream = (
        sys.stderr
        if args.command == "mcp" and args.transport == "stdio"
        else sys.stdout
    )
    configure_logging(level=cfg.log_level, stream=log_stream)

    try:
        if args.command == "mcp":
            asyncio.run(
                run_mcp(cfg, transport=args.transport, host=args.host, port=args.port)
            )
        else:
            asyncio.run(run_hub(cfg))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
