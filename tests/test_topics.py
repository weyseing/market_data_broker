"""Topic naming round-trip and validation tests."""

from __future__ import annotations

import pytest

from market_data_broker.topics import (
    COINBASE_CHANNELS_IN_SCOPE,
    InvalidTopicError,
    Topic,
    is_valid_topic,
    parse_topic,
    topic_for,
    validate_coinbase_channel,
)


class TestTopicFor:
    def test_builds_canonical_form(self) -> None:
        assert topic_for("coinbase", "ticker", "BTC-USD") == "coinbase.ticker.BTC-USD"

    def test_supports_underscored_channel(self) -> None:
        assert topic_for("coinbase", "level2_batch", "BTC-USD") == "coinbase.level2_batch.BTC-USD"

    def test_supports_numeric_product(self) -> None:
        assert topic_for("coinbase", "ticker", "1INCH-USD") == "coinbase.ticker.1INCH-USD"

    @pytest.mark.parametrize("venue", ["", "Coinbase", "1coinbase", "coin base", "coinbase!"])
    def test_rejects_bad_venue(self, venue: str) -> None:
        with pytest.raises(InvalidTopicError, match="invalid venue"):
            topic_for(venue, "ticker", "BTC-USD")

    @pytest.mark.parametrize("channel", ["", "Ticker", "ticker.sub", "1ticker", "ticker-x"])
    def test_rejects_bad_channel(self, channel: str) -> None:
        with pytest.raises(InvalidTopicError, match="invalid channel"):
            topic_for("coinbase", channel, "BTC-USD")

    @pytest.mark.parametrize(
        "product_id",
        ["", "btc-usd", "BTCUSD", "BTC-USD-X", "BTC USD", "BTC--USD", "BTC-"],
    )
    def test_rejects_bad_product_id(self, product_id: str) -> None:
        with pytest.raises(InvalidTopicError, match="invalid product_id"):
            topic_for("coinbase", "ticker", product_id)


class TestParseTopic:
    def test_round_trip(self) -> None:
        for venue, channel, product in [
            ("coinbase", "ticker", "BTC-USD"),
            ("coinbase", "matches", "ETH-USD"),
            ("coinbase", "level2_batch", "SOL-USD"),
            ("binance", "ticker", "BTC-USDT"),
        ]:
            topic = topic_for(venue, channel, product)
            parsed = parse_topic(topic)
            assert parsed == Topic(venue, channel, product)
            assert topic_for(*parsed) == topic

    def test_rejects_too_few_parts(self) -> None:
        with pytest.raises(InvalidTopicError, match="3 dot-separated parts"):
            parse_topic("coinbase.ticker")

    def test_rejects_too_many_parts(self) -> None:
        with pytest.raises(InvalidTopicError, match="3 dot-separated parts"):
            parse_topic("coinbase.ticker.BTC-USD.extra")

    def test_rejects_lowercase_product(self) -> None:
        with pytest.raises(InvalidTopicError):
            parse_topic("coinbase.ticker.btc-usd")

    def test_rejects_empty_string(self) -> None:
        with pytest.raises(InvalidTopicError):
            parse_topic("")


class TestIsValidTopic:
    def test_valid(self) -> None:
        assert is_valid_topic("coinbase.ticker.BTC-USD") is True

    def test_invalid(self) -> None:
        assert is_valid_topic("coinbase.ticker.btc-usd") is False
        assert is_valid_topic("nope") is False


class TestValidateCoinbaseChannel:
    def test_in_scope_channels_pass(self) -> None:
        for ch in COINBASE_CHANNELS_IN_SCOPE:
            validate_coinbase_channel(ch)  # does not raise

    def test_in_scope_set_is_what_plan_says(self) -> None:
        assert COINBASE_CHANNELS_IN_SCOPE == frozenset({"ticker", "matches", "level2_batch"})

    def test_unknown_channel_rejected(self) -> None:
        with pytest.raises(InvalidTopicError, match="not in scope"):
            validate_coinbase_channel("orderbook")

    def test_heartbeat_not_in_scope(self) -> None:
        # Heartbeat is used internally for liveness, not exposed as a topic.
        with pytest.raises(InvalidTopicError):
            validate_coinbase_channel("heartbeat")
