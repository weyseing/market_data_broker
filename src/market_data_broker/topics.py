"""Topic naming for the broker.

Wire format: ``{venue}.{channel}.{product_id}``  e.g. ``coinbase.ticker.BTC-USD``

The venue prefix leaves room for future venues (binance, kraken) without breaking
existing consumers. Topics are the routing key on the in-memory bus and the unit
of reference-counting in the registry.
"""

from __future__ import annotations

import re
from typing import NamedTuple

VENUE_COINBASE = "coinbase"

COINBASE_CHANNELS_IN_SCOPE: frozenset[str] = frozenset({"ticker", "matches", "level2_batch"})

_VENUE_RE = re.compile(r"^[a-z][a-z0-9_]*$")
_CHANNEL_RE = re.compile(r"^[a-z][a-z0-9_]*$")
_PRODUCT_ID_RE = re.compile(r"^[A-Z0-9]+-[A-Z0-9]+$")


class InvalidTopicError(ValueError):
    """Raised when a topic string or component fails validation."""


class Topic(NamedTuple):
    venue: str
    channel: str
    product_id: str


def topic_for(venue: str, channel: str, product_id: str) -> str:
    """Build a topic from its parts. Validates format only — does not check
    whether the channel is in scope for the venue (use :func:`validate_coinbase_channel`
    for that)."""
    if not _VENUE_RE.match(venue):
        raise InvalidTopicError(f"invalid venue {venue!r}: must match {_VENUE_RE.pattern}")
    if not _CHANNEL_RE.match(channel):
        raise InvalidTopicError(f"invalid channel {channel!r}: must match {_CHANNEL_RE.pattern}")
    if not _PRODUCT_ID_RE.match(product_id):
        raise InvalidTopicError(
            f"invalid product_id {product_id!r}: must match {_PRODUCT_ID_RE.pattern}"
        )
    return f"{venue}.{channel}.{product_id}"


def parse_topic(topic: str) -> Topic:
    """Inverse of :func:`topic_for`. Raises :class:`InvalidTopicError` if the string
    does not round-trip cleanly."""
    parts = topic.split(".")
    if len(parts) != 3:
        raise InvalidTopicError(
            f"topic {topic!r} must have exactly 3 dot-separated parts (venue.channel.product_id)"
        )
    venue, channel, product_id = parts
    # Re-build to enforce per-part regexes uniformly.
    if topic_for(venue, channel, product_id) != topic:
        raise InvalidTopicError(f"topic {topic!r} failed round-trip validation")
    return Topic(venue, channel, product_id)


def is_valid_topic(topic: str) -> bool:
    try:
        parse_topic(topic)
    except InvalidTopicError:
        return False
    return True


def validate_coinbase_channel(channel: str) -> None:
    """Raise if ``channel`` is not one of the Coinbase channels this hub supports."""
    if channel not in COINBASE_CHANNELS_IN_SCOPE:
        raise InvalidTopicError(
            f"channel {channel!r} not in scope for coinbase; "
            f"expected one of {sorted(COINBASE_CHANNELS_IN_SCOPE)}"
        )
