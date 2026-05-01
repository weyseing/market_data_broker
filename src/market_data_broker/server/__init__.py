"""Downstream-facing servers: the surfaces non-publisher consumers connect to.

One module per protocol. ``ws`` is the plain WebSocket streaming endpoint for
language-agnostic clients (``websocat``, browser code, etc.); the MCP server
(Step 8) will live here as a sibling.

The servers are thin: they translate the wire protocol into calls against the
:class:`~market_data_broker.registry.Registry`, then forward bus messages back
to the client. All ref-counting, demand-driven upstream sub/unsub, and message
routing happens in the layers below — adding a new protocol means writing a new
adapter, not changing the hub.
"""

from .ws import DownstreamWSServer

__all__ = ["DownstreamWSServer"]
