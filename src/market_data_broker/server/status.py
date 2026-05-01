"""HTTP ``/status`` endpoint.

A tiny single-route HTTP server that returns ``registry.status()`` as JSON.
Mirrors the upcoming MCP ``get_hub_status`` tool (Step 8) but accessible to any
HTTP client — operators with curl, container health checks, scrapers.

Hand-rolled HTTP/1.1 over :func:`asyncio.start_server` to avoid pulling in
aiohttp for one route. We only handle the minimum:

- ``GET /status``  → 200 + JSON (the full hub status)
- ``GET /healthz`` → 200 + ``{"status":"ok"}`` (process-alive ping; doesn't
  introspect ingest health — use ``/status`` for that)
- Anything else    → 404
- Non-GET methods  → 405 (with ``Allow: GET`` header)
- Malformed input  → 400, connection closed
- Unhandled errors → 500, connection closed (logged)

Connections are short-lived and one-shot — every response sets
``Connection: close``. Each request gets its own task; the server owns no
shared state between requests.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
from typing import TYPE_CHECKING, Any

from ..logging_config import get_logger

if TYPE_CHECKING:
    from ..registry import Registry

DEFAULT_HOST = "0.0.0.0"
DEFAULT_PORT = 8080

# Each request is tiny (<1 KiB). Cap how much we'll buffer before giving up
# — protects against amok clients holding the connection open with infinite
# headers. Server is internal-facing but defence-in-depth is cheap.
_MAX_HEADER_BYTES = 8 * 1024
_READ_TIMEOUT_SECONDS = 5.0

_STATUS_REASON: dict[int, str] = {
    200: "OK",
    400: "Bad Request",
    404: "Not Found",
    405: "Method Not Allowed",
    500: "Internal Server Error",
}


class _BadRequestError(Exception):
    """Internal — translates to HTTP 400."""


class StatusHTTPServer:
    """Tiny HTTP server exposing ``GET /status`` over ``asyncio.start_server``.

    Lifecycle::

        server = StatusHTTPServer(registry)
        await server.start()
        ...
        await server.stop()

    ``start`` is idempotent; ``stop`` closes the listener and waits for any
    in-flight request handlers to drain.
    """

    def __init__(
        self,
        registry: Registry,
        *,
        host: str = DEFAULT_HOST,
        port: int = DEFAULT_PORT,
    ) -> None:
        self._registry = registry
        self._host = host
        self._port = port
        self._server: Any = None
        self._log = get_logger("server.status")

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def start(self) -> None:
        if self._server is not None:
            return
        self._server = await asyncio.start_server(
            self._handler, self._host, self._port
        )
        self._log.info("status_server.started", host=self._host, port=self.port)

    async def stop(self) -> None:
        if self._server is None:
            return
        self._server.close()
        await self._server.wait_closed()
        self._server = None
        self._log.info("status_server.stopped")

    @property
    def port(self) -> int:
        """Actual bound port. Useful when constructed with ``port=0``."""
        if self._server is None:
            return self._port
        sockets = self._server.sockets
        if not sockets:
            return self._port
        return sockets[0].getsockname()[1]

    # ------------------------------------------------------------------
    # Per-connection handler
    # ------------------------------------------------------------------

    async def _handler(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        try:
            try:
                method, path = await self._read_request(reader)
            except _BadRequestError as exc:
                await self._respond(writer, 400, {"error": str(exc)})
                return

            if method != "GET":
                await self._respond(
                    writer,
                    405,
                    {"error": f"method {method} not allowed"},
                    extra_headers=("Allow: GET\r\n",),
                )
                return

            if path == "/status":
                await self._respond(writer, 200, self._registry.status())
                return
            if path == "/healthz":
                await self._respond(writer, 200, {"status": "ok"})
                return
            await self._respond(writer, 404, {"error": f"unknown path {path!r}"})
        except Exception as exc:  # noqa: BLE001 - never tear down the loop
            self._log.warning(
                "status_server.handler_error",
                error_type=type(exc).__name__,
                error=str(exc),
            )
            with contextlib.suppress(Exception):
                await self._respond(writer, 500, {"error": "internal error"})
        finally:
            with contextlib.suppress(Exception):
                writer.close()
                await writer.wait_closed()

    # ------------------------------------------------------------------
    # Wire I/O
    # ------------------------------------------------------------------

    async def _read_request(
        self, reader: asyncio.StreamReader
    ) -> tuple[str, str]:
        """Read just enough of the HTTP request to know the method + path.
        Headers are drained but discarded — we don't accept request bodies on
        any route, so there's nothing to inspect."""
        try:
            line = await asyncio.wait_for(
                reader.readuntil(b"\r\n"), timeout=_READ_TIMEOUT_SECONDS
            )
        except TimeoutError as exc:
            raise _BadRequestError("timed out reading request line") from exc
        except asyncio.IncompleteReadError as exc:
            raise _BadRequestError("incomplete request") from exc

        try:
            method, path, version = line.decode("ascii").rstrip("\r\n").split(" ", 2)
        except (UnicodeDecodeError, ValueError) as exc:
            raise _BadRequestError("malformed request line") from exc
        if not version.startswith("HTTP/"):
            raise _BadRequestError("unsupported protocol")

        total = len(line)
        while True:
            try:
                hdr = await asyncio.wait_for(
                    reader.readuntil(b"\r\n"), timeout=_READ_TIMEOUT_SECONDS
                )
            except TimeoutError as exc:
                raise _BadRequestError("timed out reading headers") from exc
            except asyncio.IncompleteReadError as exc:
                raise _BadRequestError("incomplete headers") from exc
            total += len(hdr)
            if total > _MAX_HEADER_BYTES:
                raise _BadRequestError("headers too large")
            if hdr == b"\r\n":
                break
        return method, path

    async def _respond(
        self,
        writer: asyncio.StreamWriter,
        status: int,
        body: Any,
        *,
        extra_headers: tuple[str, ...] = (),
    ) -> None:
        payload = json.dumps(body).encode("utf-8")
        reason = _STATUS_REASON.get(status, "OK")
        head = (
            f"HTTP/1.1 {status} {reason}\r\n"
            f"Content-Type: application/json\r\n"
            f"Content-Length: {len(payload)}\r\n"
            f"Connection: close\r\n"
        )
        for h in extra_headers:
            head += h
        head += "\r\n"
        writer.write(head.encode("ascii"))
        writer.write(payload)
        await writer.drain()
