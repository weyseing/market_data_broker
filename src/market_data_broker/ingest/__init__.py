"""Ingestion adapters: turn upstream venue feeds into Messages on the bus.

One module per venue. Each adapter implements the same shape: it owns a single
WS connection (or small pool), consumes :class:`Registry` callbacks for
demand-driven (un)subscribe, and publishes parsed :class:`Message` envelopes
onto the in-memory bus.

The registry / bus / topics layers stay venue-agnostic; everything
Coinbase-specific lives in :mod:`.coinbase`. Adding a new venue means writing a
new sibling module — no changes elsewhere.
"""

from .coinbase import CoinbaseIngest

__all__ = ["CoinbaseIngest"]
