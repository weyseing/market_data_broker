"""Downstream-facing servers: the surfaces non-publisher consumers connect to.

One module per protocol:

- :mod:`ws` — plain WebSocket streaming endpoint for language-agnostic clients
  (``websocat``, browser code, internal services).
- :mod:`status` — tiny HTTP ``/status`` + ``/healthz`` endpoint for operators
  and container health checks.
- :mod:`mcp_server` — MCP server for LLM agents (stdio + streamable-HTTP).

The servers are thin: they translate the wire protocol into calls against the
:class:`~market_data_broker.registry.Registry`, then forward bus messages or
status snapshots back to the client. All ref-counting, demand-driven upstream
sub/unsub, and message routing happens in the layers below — adding a new
protocol means writing a new adapter, not changing the hub.
"""

from .mcp_server import MarketDataMCPServer
from .status import StatusHTTPServer
from .ws import DownstreamWSServer

__all__ = ["DownstreamWSServer", "MarketDataMCPServer", "StatusHTTPServer"]
