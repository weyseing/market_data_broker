"""Domain models for normalised market-data messages.

All numerics are :class:`~decimal.Decimal` — never ``float`` — because Coinbase
sends prices and sizes as JSON strings and we must preserve them exactly through
the entire pipeline. Floats would corrupt prices at BTC-scale notionals.

The :class:`Message` envelope is the universal unit on the in-memory bus:
``topic`` for routing, ``payload`` for the typed body, ``received_at`` for
latency tracking.

The wire shapes here come from the Step-1.5 spike — see
``progress/20260425_spike_coinbase_findings.md``.
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator

from .topics import VENUE_COINBASE, topic_for

# ``extra="ignore"``: Coinbase has historically added fields without versioning
# (e.g. volume_30d). Ignoring extras keeps the parser resilient to additive
# upstream changes; tests assert presence of every field we actually depend on.
_MODEL_CONFIG = ConfigDict(frozen=True, extra="ignore")


class Ticker(BaseModel):
    """One ticker tick: the trade that just printed plus post-trade top-of-book."""

    model_config = _MODEL_CONFIG

    type: Literal["ticker"]
    product_id: str
    sequence: int
    trade_id: int
    time: datetime
    price: Decimal
    last_size: Decimal
    side: Literal["buy", "sell"]
    best_bid: Decimal
    best_bid_size: Decimal
    best_ask: Decimal
    best_ask_size: Decimal
    open_24h: Decimal
    high_24h: Decimal
    low_24h: Decimal
    volume_24h: Decimal
    volume_30d: Decimal


class Trade(BaseModel):
    """One executed trade from the matches feed.

    Coinbase emits ``last_match`` for the first frame after subscribe (a catch-up
    of the most recent trade) and ``match`` for subsequent live trades. Both
    carry identical fields; we keep both literals so the discriminator is honest
    about what arrived on the wire.
    """

    model_config = _MODEL_CONFIG

    type: Literal["match", "last_match"]
    product_id: str
    sequence: int
    trade_id: int
    time: datetime
    price: Decimal
    size: Decimal
    side: Literal["buy", "sell"]
    maker_order_id: str
    taker_order_id: str


class L2Change(BaseModel):
    """One level-2 book change. ``size == 0`` means remove this price level."""

    model_config = _MODEL_CONFIG

    side: Literal["buy", "sell"]
    price: Decimal
    size: Decimal


class L2Update(BaseModel):
    """An incremental level-2 book update (one or more changes)."""

    model_config = _MODEL_CONFIG

    type: Literal["l2update"]
    product_id: str
    time: datetime
    changes: list[L2Change]

    @field_validator("changes", mode="before")
    @classmethod
    def _coerce_change_tuples(cls, v: object) -> object:
        # Coinbase sends each change as ``[side, price, size]``; map to dict.
        if not isinstance(v, list):
            return v
        out: list[object] = []
        for c in v:
            if isinstance(c, (list, tuple)) and len(c) == 3:
                out.append({"side": c[0], "price": c[1], "size": c[2]})
            else:
                out.append(c)
        return out


class L2Snapshot(BaseModel):
    """Full top-N order book snapshot. Sent once per product on subscription.

    Note: Coinbase aliases ``level2_batch`` to ``level2_50``, so this snapshot
    carries up to 50 levels per side, not full depth.
    """

    model_config = _MODEL_CONFIG

    type: Literal["snapshot"]
    product_id: str
    asks: list[tuple[Decimal, Decimal]]
    bids: list[tuple[Decimal, Decimal]]


Payload = Ticker | Trade | L2Update | L2Snapshot


class Message(BaseModel):
    """Universal envelope on the bus. ``topic`` is the routing key; ``payload``
    is one of the venue-specific typed bodies."""

    model_config = ConfigDict(frozen=True)

    topic: str
    venue: str
    received_at: datetime
    payload: Payload = Field(discriminator="type")


# ---------------------------------------------------------------------------
# Coinbase frame parsing
# ---------------------------------------------------------------------------

_TYPE_TO_PARSER: dict[str, type[BaseModel]] = {
    "ticker": Ticker,
    "match": Trade,
    "last_match": Trade,
    "l2update": L2Update,
    "snapshot": L2Snapshot,
}

_TYPE_TO_CHANNEL: dict[str, str] = {
    "ticker": "ticker",
    "match": "matches",
    "last_match": "matches",
    "l2update": "level2_batch",
    "snapshot": "level2_batch",
}


class UnknownFrameTypeError(ValueError):
    """Raised when a Coinbase frame's ``type`` is not one we model.

    Callers should filter out non-data frames (``subscriptions``, ``heartbeat``,
    ``error``) before calling :func:`parse_coinbase_frame`.
    """


def parse_coinbase_frame(raw: dict, *, received_at: datetime | None = None) -> Message:
    """Validate a raw Coinbase WS frame and wrap it in a :class:`Message`.

    ``received_at`` defaults to ``datetime.now(timezone.utc)`` so production
    callers get sensible latency timestamps; tests can pin it.
    """
    frame_type = raw.get("type")
    parser = _TYPE_TO_PARSER.get(frame_type) if isinstance(frame_type, str) else None
    if parser is None:
        raise UnknownFrameTypeError(
            f"unrecognised coinbase frame type: {frame_type!r}; "
            f"caller must filter non-data frames before parsing"
        )
    payload = parser.model_validate(raw)
    channel = _TYPE_TO_CHANNEL[frame_type]  # type: ignore[index]  # frame_type is str here
    product_id = raw["product_id"]
    topic = topic_for(VENUE_COINBASE, channel, product_id)
    return Message(
        topic=topic,
        venue=VENUE_COINBASE,
        received_at=received_at or datetime.now(UTC),
        payload=payload,  # type: ignore[arg-type]  # parser narrows to the right Payload member
    )
