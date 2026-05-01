"""Connect a WS client to a running hub, subscribe, print N frames, exit.

Used as a stand-in for ``websocat`` in the Step 7 manual verify when websocat
isn't installed locally.

Usage::

    python scripts/smoke_ws_client.py [topic [n_frames]]
"""

from __future__ import annotations

import asyncio
import json
import sys

from websockets.asyncio.client import connect


async def main(topic: str, n_frames: int) -> None:
    async with connect("ws://127.0.0.1:8765", max_size=2 * 1024 * 1024) as ws:
        welcome = json.loads(await ws.recv())
        print("welcome:", welcome)
        await ws.send(json.dumps({"action": "subscribe", "topics": [topic]}))
        seen = 0
        while seen < n_frames:
            frame = json.loads(await ws.recv())
            kind = frame.get("kind")
            if kind == "ack":
                print("ack:", frame)
                continue
            if kind == "error":
                print("error:", frame)
                continue
            if kind == "data":
                seen += 1
                print(f"data[{seen}]:", frame.get("topic"), frame.get("payload", {}).get("type"))
                continue
            print("other:", frame)
        await ws.send(json.dumps({"action": "unsubscribe", "topics": [topic]}))
        ack = json.loads(await ws.recv())
        print("unsub ack:", ack)


if __name__ == "__main__":
    topic = sys.argv[1] if len(sys.argv) > 1 else "coinbase.ticker.BTC-USD"
    n = int(sys.argv[2]) if len(sys.argv) > 2 else 3
    asyncio.run(main(topic, n))
