import json
from unittest.mock import patch

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from cex_arb.backtest import Base, OpportunityResult, main
from cex_arb.models import OrderBook


@pytest.fixture
def db_engine():
    """In-memory SQLite with the OrderBook schema."""
    engine = create_engine("sqlite://", echo=False)
    Base.metadata.create_all(engine)
    yield engine
    engine.dispose()


def _insert_ticks(engine, rows):
    session = sessionmaker(bind=engine)
    with session() as s:
        for row in rows:
            s.add(OrderBook(**row))
        s.commit()


class TestMainIntegration:
    def test_positive_spread_produces_results(self, db_engine, tmp_path, monkeypatch):
        """Durable positive spread across 2 exchanges should produce at least one OpportunityResult."""
        monkeypatch.chdir(tmp_path)

        # binance ask=100, mexc bid=101.5, roughly 1.35% spread after fees
        rows = []
        base_timestamp = 1_000_000
        for i in range(20):
            tick_timestamp =base_timestamp + i * 50
            rows.append(dict(
                timestamp=tick_timestamp, exchange="binance", ticker="PENGU",
                bid_price=100.0, bid_quantity=10.0,
                ask_price=100.0, ask_quantity=10.0,
            ))
            rows.append(dict(
                timestamp=tick_timestamp, exchange="mexc", ticker="PENGU",
                bid_price=101.5, bid_quantity=10.0,
                ask_price=102.0, ask_quantity=10.0,
            ))

        _insert_ticks(db_engine, rows)

        with patch("cex_arb.backtest.engine", db_engine):
            results = main()

        assert len(results) >= 1
        assert all(isinstance(r, OpportunityResult) for r in results)
        assert results[0].ticker == "PENGU"
        assert results[0].ask_exchange == "binance"
        assert results[0].bid_exchange == "mexc"
        assert results[0].profit_usdt > 0

    def test_no_opportunity_negative_spread(self, db_engine, tmp_path, monkeypatch):
        """Negative spread should produce empty results."""
        monkeypatch.chdir(tmp_path)

        # binance ask=100 (lowest), mexc bid=99.5 (highest), negative spread
        rows = []
        base_timestamp = 1_000_000
        for i in range(20):
            tick_timestamp =base_timestamp + i * 50
            rows.append(dict(
                timestamp=tick_timestamp, exchange="binance", ticker="PENGU",
                bid_price=99.0, bid_quantity=10.0,
                ask_price=100.0, ask_quantity=10.0,
            ))
            rows.append(dict(
                timestamp=tick_timestamp, exchange="mexc", ticker="PENGU",
                bid_price=99.5, bid_quantity=10.0,
                ask_price=100.5, ask_quantity=10.0,
            ))

        _insert_ticks(db_engine, rows)

        with patch("cex_arb.backtest.engine", db_engine):
            results = main()

        assert results == []

    def test_json_written_to_disk(self, db_engine, tmp_path, monkeypatch):
        """main() writes backtest.json with correct structure."""
        monkeypatch.chdir(tmp_path)

        rows = []
        base_timestamp = 1_000_000
        for i in range(20):
            tick_timestamp =base_timestamp + i * 50
            rows.append(dict(
                timestamp=tick_timestamp, exchange="binance", ticker="PENGU",
                bid_price=100.0, bid_quantity=10.0,
                ask_price=100.0, ask_quantity=10.0,
            ))
            rows.append(dict(
                timestamp=tick_timestamp, exchange="mexc", ticker="PENGU",
                bid_price=101.5, bid_quantity=10.0,
                ask_price=102.0, ask_quantity=10.0,
            ))

        _insert_ticks(db_engine, rows)

        with patch("cex_arb.backtest.engine", db_engine):
            results = main()

        json_path = tmp_path / "backtest.json"
        assert json_path.exists()

        data = json.loads(json_path.read_text())
        assert len(data) == len(results)

        expected_keys = {
            "ticker", "ask_exchange", "bid_exchange", "start_ts", "end_ts",
            "duration_ms", "mean_pct", "max_pct", "p80_pct", "obs", "qty",
            "profit_usdt",
        }
        for entry in data:
            assert set(entry.keys()) == expected_keys

    def test_spread_closes_when_negative(self, db_engine, tmp_path, monkeypatch):
        """Spread goes positive for 20 ticks then negative the opportunity should close normally (not flushed at end)."""
        monkeypatch.chdir(tmp_path)

        rows = []
        base_timestamp = 1_000_000
        # Phase 1: 20 ticks with positive spread (950ms, >10 obs)
        for i in range(20):
            tick_timestamp =base_timestamp + i * 50
            rows.append(dict(
                timestamp=tick_timestamp, exchange="binance", ticker="PENGU",
                bid_price=99.0, bid_quantity=10.0,
                ask_price=100.0, ask_quantity=10.0,
            ))
            rows.append(dict(
                timestamp=tick_timestamp, exchange="mexc", ticker="PENGU",
                bid_price=101.5, bid_quantity=10.0,
                ask_price=102.0, ask_quantity=10.0,
            ))
        # Phase 2: 5 ticks with negative spread which closes the opportunity
        # Keep exchanges different (binance best ask, mexc best bid) so we
        # reach the else branch instead of hitting best_bid==best_ask skip.
        for i in range(5):
            tick_timestamp =base_timestamp + (20 + i) * 50
            rows.append(dict(
                timestamp=tick_timestamp, exchange="binance", ticker="PENGU",
                bid_price=99.0, bid_quantity=10.0,
                ask_price=100.0, ask_quantity=10.0,
            ))
            rows.append(dict(
                timestamp=tick_timestamp, exchange="mexc", ticker="PENGU",
                bid_price=99.5, bid_quantity=10.0,
                ask_price=100.5, ask_quantity=10.0,
            ))

        _insert_ticks(db_engine, rows)

        with patch("cex_arb.backtest.engine", db_engine):
            results = main()

        assert len(results) == 1
        assert results[0].profit_usdt > 0

    def test_cooldown_blocks_second_opportunity(self, db_engine, tmp_path, monkeypatch):
        """After first opportunity closes, same pair within COOLDOWN_MS is blocked."""
        monkeypatch.chdir(tmp_path)

        rows = []
        base_timestamp = 1_000_000
        # Phase 1: 20 ticks positive spread so opportunity is recorded
        for i in range(20):
            tick_timestamp =base_timestamp + i * 50
            rows.append(dict(
                timestamp=tick_timestamp, exchange="binance", ticker="PENGU",
                bid_price=99.0, bid_quantity=10.0,
                ask_price=100.0, ask_quantity=10.0,
            ))
            rows.append(dict(
                timestamp=tick_timestamp, exchange="mexc", ticker="PENGU",
                bid_price=101.5, bid_quantity=10.0,
                ask_price=102.0, ask_quantity=10.0,
            ))
        # Phase 2: 5 ticks negative spread which closes the opportunity
        # cooldown set at result.start_ts + COOLDOWN_MS = 1_000_000 + 10000 = 1_010_000
        for i in range(5):
            tick_timestamp =base_timestamp + (20 + i) * 50
            rows.append(dict(
                timestamp=tick_timestamp, exchange="binance", ticker="PENGU",
                bid_price=99.0, bid_quantity=10.0,
                ask_price=100.0, ask_quantity=10.0,
            ))
            rows.append(dict(
                timestamp=tick_timestamp, exchange="mexc", ticker="PENGU",
                bid_price=99.5, bid_quantity=10.0,
                ask_price=100.5, ask_quantity=10.0,
            ))
        # Phase 3: 20 ticks positive spread again, but within cooldown window
        # these start at base_timestamp + 25*50 = 1_001_250 < 1_010_000 so cooldown is still active
        for i in range(20):
            tick_timestamp =base_timestamp + (25 + i) * 50
            rows.append(dict(
                timestamp=tick_timestamp, exchange="binance", ticker="PENGU",
                bid_price=99.0, bid_quantity=10.0,
                ask_price=100.0, ask_quantity=10.0,
            ))
            rows.append(dict(
                timestamp=tick_timestamp, exchange="mexc", ticker="PENGU",
                bid_price=101.5, bid_quantity=10.0,
                ask_price=102.0, ask_quantity=10.0,
            ))

        _insert_ticks(db_engine, rows)

        with patch("cex_arb.backtest.engine", db_engine):
            results = main()

        # Only the first opportunity should be recorded; the second is blocked by cooldown
        assert len(results) == 1
