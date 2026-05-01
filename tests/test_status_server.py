"""Status HTTP server — end-to-end tests.

Spawn the real server on an ephemeral port and drive it with raw asyncio
sockets. We send hand-crafted HTTP/1.1 requests and parse the responses
ourselves — gives authentic protocol behaviour without pulling in a client lib.

Pinned behaviour:
    - GET /status returns 200 + the registry.status() shape
    - GET /healthz returns 200 + {"status":"ok"}
    - Unknown path returns 404
    - Non-GET methods return 405 with Allow: GET
    - Malformed request returns 400
    - Stop closes the listener cleanly
    - Concurrent requests don't interfere
    - Status reflects real registry state (consumers, topics)
"""

from __future__ import annotations

import asyncio
import contextlib
import json

import pytest

from market_data_broker.bus import InMemoryBus
from market_data_broker.registry import Registry
from market_data_broker.server.status import StatusHTTPServer

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


async def _make_server() -> tuple[StatusHTTPServer, Registry]:
    bus = InMemoryBus()
    registry = Registry(bus)
    server = StatusHTTPServer(registry, host="127.0.0.1", port=0)
    await server.start()
    return server, registry


async def _http_request(
    host: str, port: int, method: str, path: str, *, raw: bytes | None = None
) -> tuple[int, dict[str, str], str]:
    """Send a single HTTP/1.1 request and return ``(status, headers, body)``.

    ``raw`` lets a test inject a malformed request line.
    """
    reader, writer = await asyncio.open_connection(host, port)
    try:
        if raw is not None:
            writer.write(raw)
        else:
            writer.write(
                f"{method} {path} HTTP/1.1\r\nHost: localhost\r\n\r\n".encode("ascii")
            )
        await writer.drain()

        line = await asyncio.wait_for(reader.readuntil(b"\r\n"), timeout=2.0)
        parts = line.decode("ascii").rstrip("\r\n").split(" ", 2)
        status = int(parts[1])

        headers: dict[str, str] = {}
        while True:
            h = await asyncio.wait_for(reader.readuntil(b"\r\n"), timeout=2.0)
            if h == b"\r\n":
                break
            k, _, v = h.decode("ascii").rstrip("\r\n").partition(": ")
            headers[k.lower()] = v

        length = int(headers.get("content-length", "0"))
        body_bytes = await asyncio.wait_for(
            reader.readexactly(length), timeout=2.0
        ) if length else b""
        return status, headers, body_bytes.decode("utf-8")
    finally:
        writer.close()
        with contextlib.suppress(Exception):
            await writer.wait_closed()


# ---------------------------------------------------------------------------
# Lifecycle
# ---------------------------------------------------------------------------


async def test_start_idempotent() -> None:
    server, _ = await _make_server()
    try:
        port = server.port
        await server.start()  # second call must not rebind
        assert server.port == port
    finally:
        await server.stop()


async def test_stop_idempotent() -> None:
    server, _ = await _make_server()
    await server.stop()
    await server.stop()  # second call must not raise


async def test_port_reflects_actual_bound_port() -> None:
    server, _ = await _make_server()
    try:
        assert server.port > 0
    finally:
        await server.stop()


async def test_stop_closes_listener() -> None:
    server, _ = await _make_server()
    port = server.port
    await server.stop()
    # Subsequent connection attempts must fail — listener is gone.
    with pytest.raises((ConnectionRefusedError, OSError)):
        await asyncio.wait_for(
            asyncio.open_connection("127.0.0.1", port), timeout=1.0
        )


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------


async def test_get_status_returns_200_with_registry_status() -> None:
    server, _ = await _make_server()
    try:
        status, headers, body = await _http_request(
            "127.0.0.1", server.port, "GET", "/status"
        )
        assert status == 200
        assert headers["content-type"] == "application/json"
        assert headers["connection"] == "close"
        data = json.loads(body)
        # Sanity: must contain the keys registry.status() returns.
        for key in ("hub_started_at", "hub_uptime_seconds", "upstreams", "consumers", "topics"):
            assert key in data, f"missing {key} in /status response"
    finally:
        await server.stop()


async def test_get_healthz_returns_ok() -> None:
    server, _ = await _make_server()
    try:
        status, _, body = await _http_request(
            "127.0.0.1", server.port, "GET", "/healthz"
        )
        assert status == 200
        assert json.loads(body) == {"status": "ok"}
    finally:
        await server.stop()


async def test_unknown_path_returns_404() -> None:
    server, _ = await _make_server()
    try:
        status, _, body = await _http_request(
            "127.0.0.1", server.port, "GET", "/does-not-exist"
        )
        assert status == 404
        assert "does-not-exist" in json.loads(body)["error"]
    finally:
        await server.stop()


async def test_post_returns_405_with_allow_header() -> None:
    server, _ = await _make_server()
    try:
        status, headers, _ = await _http_request(
            "127.0.0.1", server.port, "POST", "/status"
        )
        assert status == 405
        assert headers["allow"] == "GET"
    finally:
        await server.stop()


async def test_put_returns_405() -> None:
    server, _ = await _make_server()
    try:
        status, _, _ = await _http_request(
            "127.0.0.1", server.port, "PUT", "/status"
        )
        assert status == 405
    finally:
        await server.stop()


# ---------------------------------------------------------------------------
# Malformed input
# ---------------------------------------------------------------------------


async def test_malformed_request_line_returns_400() -> None:
    server, _ = await _make_server()
    try:
        status, _, body = await _http_request(
            "127.0.0.1",
            server.port,
            "",
            "",
            raw=b"NOT VALID HTTP\r\n\r\n",
        )
        assert status == 400
        assert "error" in json.loads(body)
    finally:
        await server.stop()


async def test_non_http_protocol_returns_400() -> None:
    server, _ = await _make_server()
    try:
        status, _, _ = await _http_request(
            "127.0.0.1",
            server.port,
            "",
            "",
            raw=b"GET /status FTP/1.0\r\n\r\n",
        )
        assert status == 400
    finally:
        await server.stop()


# ---------------------------------------------------------------------------
# Live state reflection
# ---------------------------------------------------------------------------


async def test_status_reflects_active_consumer() -> None:
    """A registered consumer appears in /status output."""
    server, registry = await _make_server()
    try:
        await registry.register_consumer("test-consumer-1", ["coinbase.ticker.BTC-USD"])
        _, _, body = await _http_request(
            "127.0.0.1", server.port, "GET", "/status"
        )
        data = json.loads(body)
        consumer_ids = [c["consumer_id"] for c in data["consumers"]]
        assert "test-consumer-1" in consumer_ids
        topic_names = [t["topic"] for t in data["topics"]]
        assert "coinbase.ticker.BTC-USD" in topic_names
    finally:
        await server.stop()


async def test_concurrent_requests_dont_interfere() -> None:
    server, _ = await _make_server()
    try:

        async def one() -> int:
            status, _, _ = await _http_request(
                "127.0.0.1", server.port, "GET", "/status"
            )
            return status

        statuses = await asyncio.gather(*(one() for _ in range(10)))
        assert statuses == [200] * 10
    finally:
        await server.stop()
