"""Throwaway spike: connect to Coinbase WS, subscribe BTC-USD ticker, print raw frames.

Goal: confirm real message shapes before locking in Pydantic models in Step 2.

Run:
    python scripts/spike_coinbase.py
    python scripts/spike_coinbase.py --channel matches --products BTC-USD,ETH-USD
    python scripts/spike_coinbase.py --channel level2_batch --max 5

Ctrl+C to stop. Not part of the production code path.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import signal
import sys
from contextlib import suppress

import websockets

COINBASE_WS_URL = "wss://ws-feed.exchange.coinbase.com"


async def run(url: str, channel: str, products: list[str], max_frames: int | None) -> None:
    sub = {"type": "subscribe", "product_ids": products, "channels": [channel]}
    print(f"connecting → {url}", flush=True)
    print(f"subscribing → channel={channel} products={products}", flush=True)

    # max_size=None: level2 snapshots can exceed the default 1 MB frame limit.
    async with websockets.connect(url, ping_interval=20, ping_timeout=20, max_size=None) as ws:
        await ws.send(json.dumps(sub))
        seen = 0
        async for raw in ws:
            try:
                msg = json.loads(raw)
            except json.JSONDecodeError:
                print(f"[non-json] {raw!r}", flush=True)
                continue
            print(json.dumps(msg, indent=2, sort_keys=True), flush=True)
            print("---", flush=True)
            seen += 1
            if max_frames is not None and seen >= max_frames:
                print(f"reached --max={max_frames}, exiting", flush=True)
                return


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--url", default=COINBASE_WS_URL)
    p.add_argument(
        "--channel",
        default="ticker",
        choices=["ticker", "matches", "level2_batch", "heartbeat"],
    )
    p.add_argument(
        "--products",
        default="BTC-USD",
        help="Comma-separated list of product_ids (default: BTC-USD)",
    )
    p.add_argument(
        "--max",
        type=int,
        default=None,
        help="Exit after this many frames (default: run until Ctrl+C)",
    )
    return p.parse_args()


def main() -> int:
    args = parse_args()
    products = [p.strip() for p in args.products.split(",") if p.strip()]

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    stop = asyncio.Event()
    for sig in (signal.SIGINT, signal.SIGTERM):
        with suppress(NotImplementedError):
            loop.add_signal_handler(sig, stop.set)

    async def supervise() -> None:
        run_task = asyncio.create_task(run(args.url, args.channel, products, args.max))
        stop_task = asyncio.create_task(stop.wait())
        _, pending = await asyncio.wait(
            {run_task, stop_task},
            return_when=asyncio.FIRST_COMPLETED,
        )
        for t in pending:
            t.cancel()
            with suppress(asyncio.CancelledError):
                await t

    try:
        loop.run_until_complete(supervise())
    finally:
        loop.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
