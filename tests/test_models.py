"""Domain-model tests against real Coinbase frame shapes captured in the Step-1.5 spike."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

import pytest

from market_data_broker.models import (
    L2Snapshot,
    L2Update,
    Message,
    Ticker,
    Trade,
    UnknownFrameTypeError,
    parse_coinbase_frame,
)

# ---------------------------------------------------------------------------
# Real captured payloads (from progress/20260425_spike_coinbase_findings.md)
# ---------------------------------------------------------------------------

TICKER_FRAME: dict = {
    "type": "ticker",
    "product_id": "BTC-USD",
    "sequence": 127113500279,
    "trade_id": 1008094628,
    "time": "2026-04-25T13:53:06.170361Z",
    "price": "77668.92",
    "side": "buy",
    "last_size": "0.0003128",
    "best_bid": "77668.91",
    "best_bid_size": "0.20495031",
    "best_ask": "77668.92",
    "best_ask_size": "0.49546688",
    "open_24h": "78107.84",
    "high_24h": "78257.09",
    "low_24h": "77289",
    "volume_24h": "5457.73586321",
    "volume_30d": "261517.66959213",
}

LAST_MATCH_FRAME: dict = {
    "type": "last_match",
    "product_id": "BTC-USD",
    "sequence": 127113513369,
    "trade_id": 1008094908,
    "time": "2026-04-25T13:53:49.062102Z",
    "price": "77706.48",
    "size": "0.0000001",
    "side": "buy",
    "maker_order_id": "13de9002-7b7f-4728-a51a-eb59ccbbcd43",
    "taker_order_id": "a68a6f59-4d2c-4b01-8656-4147e24e89cf",
}

MATCH_FRAME: dict = {**LAST_MATCH_FRAME, "type": "match"}

L2_SNAPSHOT_FRAME: dict = {
    "type": "snapshot",
    "product_id": "BTC-USD",
    "asks": [["77707.86", "0.34855132"], ["77709.03", "0.01544222"]],
    "bids": [["77707.85", "0.20111111"], ["77707.50", "0.10000000"]],
}

L2_UPDATE_FRAME: dict = {
    "type": "l2update",
    "product_id": "BTC-USD",
    "time": "2026-04-25T13:54:01.123456Z",
    "changes": [
        ["buy", "77697.01", "0.13838760"],
        ["buy", "77647.43", "0.00000000"],
        ["sell", "77710.00", "0.50000000"],
    ],
}


# ---------------------------------------------------------------------------
# Per-payload validation
# ---------------------------------------------------------------------------


class TestTicker:
    def test_parses_real_payload(self) -> None:
        t = Ticker.model_validate(TICKER_FRAME)
        assert t.type == "ticker"
        assert t.product_id == "BTC-USD"
        assert t.sequence == 127113500279
        assert t.side == "buy"

    def test_decimal_preserved_exactly(self) -> None:
        # Floats would round 77668.92 to a non-equal value; Decimal must not.
        t = Ticker.model_validate(TICKER_FRAME)
        assert isinstance(t.price, Decimal)
        assert t.price == Decimal("77668.92")
        assert t.best_bid == Decimal("77668.91")
        assert t.volume_30d == Decimal("261517.66959213")

    def test_time_parsed_as_utc(self) -> None:
        t = Ticker.model_validate(TICKER_FRAME)
        assert t.time.tzinfo is not None
        assert t.time.utcoffset() == UTC.utcoffset(t.time)

    def test_extra_fields_ignored(self) -> None:
        # Coinbase has historically added fields; parsing must remain resilient.
        Ticker.model_validate({**TICKER_FRAME, "future_field": "anything"})

    def test_rejects_wrong_type(self) -> None:
        with pytest.raises(Exception):  # noqa: B017 — pydantic ValidationError
            Ticker.model_validate({**TICKER_FRAME, "type": "match"})


class TestTrade:
    def test_parses_match(self) -> None:
        t = Trade.model_validate(MATCH_FRAME)
        assert t.type == "match"
        assert t.price == Decimal("77706.48")
        assert t.maker_order_id == "13de9002-7b7f-4728-a51a-eb59ccbbcd43"

    def test_parses_last_match(self) -> None:
        t = Trade.model_validate(LAST_MATCH_FRAME)
        assert t.type == "last_match"

    def test_rejects_unrelated_type(self) -> None:
        with pytest.raises(Exception):  # noqa: B017
            Trade.model_validate({**MATCH_FRAME, "type": "ticker"})


class TestL2Update:
    def test_parses_changes_as_tuples(self) -> None:
        u = L2Update.model_validate(L2_UPDATE_FRAME)
        assert len(u.changes) == 3
        assert u.changes[0].side == "buy"
        assert u.changes[0].price == Decimal("77697.01")
        assert u.changes[0].size == Decimal("0.13838760")

    def test_size_zero_means_removal(self) -> None:
        u = L2Update.model_validate(L2_UPDATE_FRAME)
        assert u.changes[1].size == Decimal("0")

    def test_changes_decimals_preserved(self) -> None:
        u = L2Update.model_validate(L2_UPDATE_FRAME)
        for c in u.changes:
            assert isinstance(c.price, Decimal)
            assert isinstance(c.size, Decimal)


class TestL2Snapshot:
    def test_parses_levels(self) -> None:
        s = L2Snapshot.model_validate(L2_SNAPSHOT_FRAME)
        assert s.product_id == "BTC-USD"
        assert s.asks[0] == (Decimal("77707.86"), Decimal("0.34855132"))
        assert s.bids[0] == (Decimal("77707.85"), Decimal("0.20111111"))


# ---------------------------------------------------------------------------
# Frame parsing / envelope construction
# ---------------------------------------------------------------------------


class TestParseCoinbaseFrame:
    @pytest.fixture
    def fixed_now(self) -> datetime:
        return datetime(2026, 4, 25, 14, 0, 0, tzinfo=UTC)

    def test_ticker_topic(self, fixed_now: datetime) -> None:
        msg = parse_coinbase_frame(TICKER_FRAME, received_at=fixed_now)
        assert msg.topic == "coinbase.ticker.BTC-USD"
        assert msg.venue == "coinbase"
        assert msg.received_at == fixed_now
        assert isinstance(msg.payload, Ticker)

    def test_match_routes_to_matches_channel(self, fixed_now: datetime) -> None:
        msg = parse_coinbase_frame(MATCH_FRAME, received_at=fixed_now)
        assert msg.topic == "coinbase.matches.BTC-USD"
        assert isinstance(msg.payload, Trade)

    def test_last_match_routes_to_matches_channel(self, fixed_now: datetime) -> None:
        msg = parse_coinbase_frame(LAST_MATCH_FRAME, received_at=fixed_now)
        assert msg.topic == "coinbase.matches.BTC-USD"

    def test_l2update_routes_to_level2_batch(self, fixed_now: datetime) -> None:
        msg = parse_coinbase_frame(L2_UPDATE_FRAME, received_at=fixed_now)
        assert msg.topic == "coinbase.level2_batch.BTC-USD"
        assert isinstance(msg.payload, L2Update)

    def test_snapshot_routes_to_level2_batch(self, fixed_now: datetime) -> None:
        msg = parse_coinbase_frame(L2_SNAPSHOT_FRAME, received_at=fixed_now)
        assert msg.topic == "coinbase.level2_batch.BTC-USD"
        assert isinstance(msg.payload, L2Snapshot)

    def test_received_at_defaults_to_now(self) -> None:
        before = datetime.now(UTC)
        msg = parse_coinbase_frame(TICKER_FRAME)
        after = datetime.now(UTC)
        assert before <= msg.received_at <= after

    def test_unknown_type_rejected(self) -> None:
        with pytest.raises(UnknownFrameTypeError, match="unrecognised"):
            parse_coinbase_frame({"type": "subscriptions", "product_id": "BTC-USD"})

    def test_missing_type_rejected(self) -> None:
        with pytest.raises(UnknownFrameTypeError):
            parse_coinbase_frame({"product_id": "BTC-USD"})

    def test_heartbeat_not_parsed(self) -> None:
        # Heartbeat frames are caller-filtered; parser should reject them.
        with pytest.raises(UnknownFrameTypeError):
            parse_coinbase_frame({"type": "heartbeat", "product_id": "BTC-USD"})


class TestMessageEnvelope:
    def test_round_trip_json_preserves_decimals(self) -> None:
        msg = parse_coinbase_frame(
            TICKER_FRAME,
            received_at=datetime(2026, 4, 25, 14, 0, tzinfo=UTC),
        )
        round_tripped = Message.model_validate_json(msg.model_dump_json())
        assert round_tripped.topic == msg.topic
        assert isinstance(round_tripped.payload, Ticker)
        assert round_tripped.payload.price == Decimal("77668.92")

    def test_discriminator_picks_right_payload_type(self) -> None:
        msg = Message.model_validate(
            {
                "topic": "coinbase.matches.BTC-USD",
                "venue": "coinbase",
                "received_at": "2026-04-25T14:00:00Z",
                "payload": MATCH_FRAME,
            }
        )
        assert isinstance(msg.payload, Trade)
