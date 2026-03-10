import pytest
from cex_arb.backtest import (
    OpportunityResult,
    compute_net_spread_pct,
    find_best_exchanges,
    compute_profit_usd,
    evaluate_opportunity,
    fmt_ts,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

FEES = {
    "binance": 0.0010,
    "coinbase": 0.0060,
    "mexc": 0.0005,
}


def _make_tick(timestamp, spread_pct, ask_price=100.0, bid_price=101.0,
               ask_quantity=10.0, bid_quantity=10.0):
    return {
        "timestamp": timestamp,
        "spread_pct": spread_pct,
        "ask_price": ask_price,
        "ask_quantity": ask_quantity,
        "bid_price": bid_price,
        "bid_quantity": bid_quantity,
    }


def _make_ticks(count, start_timestamp=1000, interval=50, spread_pct=0.5,
                ask_price=100.0, bid_price=101.0, ask_quantity=10.0, bid_quantity=10.0):
    return [
        _make_tick(start_timestamp + i * interval, spread_pct,
                   ask_price, bid_price, ask_quantity, bid_quantity)
        for i in range(count)
    ]


# ---------------------------------------------------------------------------
# compute_net_spread_pct
# ---------------------------------------------------------------------------

class TestComputeSpreadPct:
    def test_positive_spread(self):
        # bid higher than ask after fees, expect positive spread
        result = compute_net_spread_pct(
            bid_price=101.0, ask_price=100.0, bid_fee=0.001, ask_fee=0.001,
        )
        assert result > 0

    def test_negative_spread(self):
        # bid lower than ask, expect negative spread
        result = compute_net_spread_pct(
            bid_price=99.0, ask_price=100.0, bid_fee=0.001, ask_fee=0.001,
        )
        assert result < 0

    def test_zero_fees_breakeven(self):
        # same price, zero fees, expect 0%
        result = compute_net_spread_pct(
            bid_price=100.0, ask_price=100.0, bid_fee=0.0, ask_fee=0.0,
        )
        assert result == pytest.approx(0.0)

    def test_exact_value(self):
        # manual calculation: (101 * 0.999) / (100 * 1.001) - 1
        expected = (101.0 * 0.999 / (100.0 * 1.001) - 1) * 100
        result = compute_net_spread_pct(
            bid_price=101.0, ask_price=100.0, bid_fee=0.001, ask_fee=0.001,
        )
        assert result == pytest.approx(expected)

    def test_high_fees_kill_spread(self):
        # fees eat up all the spread
        result = compute_net_spread_pct(
            bid_price=101.0, ask_price=100.0, bid_fee=0.006, ask_fee=0.006,
        )
        assert result < 0

    def test_asymmetric_fees(self):
        # mexc (0.05%) vs coinbase (0.60%)
        result = compute_net_spread_pct(
            bid_price=101.0, ask_price=100.0, bid_fee=0.0005, ask_fee=0.006,
        )
        result_sym = compute_net_spread_pct(
            bid_price=101.0, ask_price=100.0, bid_fee=0.006, ask_fee=0.0005,
        )
        # higher fee on bid side is worse (bid_price > ask_price means larger absolute impact)
        assert result > result_sym


# ---------------------------------------------------------------------------
# find_best_exchanges
# ---------------------------------------------------------------------------

class TestFindBestExchanges:
    def test_basic(self):
        book = {
            "binance": {"ask_price": 100.0, "bid_price": 99.5},
            "coinbase": {"ask_price": 99.0, "bid_price": 98.0},
            "mexc": {"ask_price": 101.0, "bid_price": 100.5},
        }
        ask_ex, bid_ex = find_best_exchanges(book)
        assert ask_ex == "coinbase"  # lowest ask
        assert bid_ex == "mexc"      # highest bid

    def test_two_exchanges(self):
        book = {
            "binance": {"ask_price": 100.0, "bid_price": 101.0},
            "coinbase": {"ask_price": 102.0, "bid_price": 99.0},
        }
        ask_ex, bid_ex = find_best_exchanges(book)
        assert ask_ex == "binance"
        assert bid_ex == "binance"

    def test_same_exchange_best_both(self):
        book = {
            "binance": {"ask_price": 90.0, "bid_price": 110.0},
            "coinbase": {"ask_price": 100.0, "bid_price": 100.0},
        }
        ask_ex, bid_ex = find_best_exchanges(book)
        assert ask_ex == "binance"
        assert bid_ex == "binance"


# ---------------------------------------------------------------------------
# compute_profit_usd
# ---------------------------------------------------------------------------

class TestComputeProfit:
    def test_basic_profit(self):
        tick = {
            "ask_price": 100.0, "ask_quantity": 10.0,
            "bid_price": 101.0, "bid_quantity": 10.0,
        }
        quantity, profit = compute_profit_usd(tick, ask_fee=0.001, bid_fee=0.001)
        assert quantity == 10.0
        expected = 10.0 * 101.0 * 0.999 - 10.0 * 100.0 * 1.001
        assert profit == pytest.approx(expected)

    def test_qty_is_min_of_both_sides(self):
        tick = {
            "ask_price": 100.0, "ask_quantity": 5.0,
            "bid_price": 101.0, "bid_quantity": 20.0,
        }
        quantity, _ = compute_profit_usd(tick, ask_fee=0.001, bid_fee=0.001)
        assert quantity == 5.0

    def test_qty_min_bid_side(self):
        tick = {
            "ask_price": 100.0, "ask_quantity": 20.0,
            "bid_price": 101.0, "bid_quantity": 3.0,
        }
        quantity, _ = compute_profit_usd(tick, ask_fee=0.001, bid_fee=0.001)
        assert quantity == 3.0

    def test_zero_fees(self):
        tick = {
            "ask_price": 100.0, "ask_quantity": 10.0,
            "bid_price": 102.0, "bid_quantity": 10.0,
        }
        quantity, profit = compute_profit_usd(tick, ask_fee=0.0, bid_fee=0.0)
        assert profit == pytest.approx(10.0 * 2.0)  # 10 * (102 - 100)

    def test_negative_profit(self):
        tick = {
            "ask_price": 100.0, "ask_quantity": 10.0,
            "bid_price": 99.0, "bid_quantity": 10.0,
        }
        _, profit = compute_profit_usd(tick, ask_fee=0.001, bid_fee=0.001)
        assert profit < 0


# ---------------------------------------------------------------------------
# evaluate_opportunity
# ---------------------------------------------------------------------------

class TestEvaluateOpportunity:
    def test_valid_opportunity(self):
        ticks = _make_ticks(20, start_timestamp=1000, interval=50, spread_pct=0.5)
        result = evaluate_opportunity(
            ticks, "PENGU", "binance", "mexc", FEES,
            min_duration_ms=400, min_observations=10, min_mean_pct=0.1,
        )
        assert result is not None
        assert isinstance(result, OpportunityResult)
        assert result.ticker == "PENGU"
        assert result.ask_exchange == "binance"
        assert result.bid_exchange == "mexc"
        assert result.duration_ms == 950  # (20-1)*50
        assert result.obs == 20
        assert result.mean_pct == pytest.approx(0.5)
        assert result.max_pct == pytest.approx(0.5)

    def test_filtered_by_duration(self):
        # 5 ticks * 50ms = 200ms duration, below 400ms threshold
        ticks = _make_ticks(5, start_timestamp=1000, interval=50, spread_pct=0.5)
        result = evaluate_opportunity(
            ticks, "PENGU", "binance", "mexc", FEES,
            min_duration_ms=400, min_observations=3, min_mean_pct=0.1,
        )
        assert result is None

    def test_filtered_by_obs(self):
        # enough duration but only 5 obs, below 10 threshold
        ticks = _make_ticks(5, start_timestamp=1000, interval=200, spread_pct=0.5)
        result = evaluate_opportunity(
            ticks, "PENGU", "binance", "mexc", FEES,
            min_duration_ms=400, min_observations=10, min_mean_pct=0.1,
        )
        assert result is None

    def test_filtered_by_mean_pct(self):
        # mean spread 0.05% < 0.1% threshold
        ticks = _make_ticks(20, start_timestamp=1000, interval=50, spread_pct=0.05)
        result = evaluate_opportunity(
            ticks, "PENGU", "binance", "mexc", FEES,
            min_duration_ms=400, min_observations=10, min_mean_pct=0.1,
        )
        assert result is None

    def test_p80_calculation(self):
        # 10 ticks with varying spreads
        ticks = [_make_tick(1000 + i * 100, spread_pct=0.1 * (i + 1))
                 for i in range(10)]
        # spreads: 0.1, 0.2, ..., 1.0
        # sorted: [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0]
        # p80 index = int(10 * 0.8) = 8, so value is 0.9
        result = evaluate_opportunity(
            ticks, "PENGU", "binance", "mexc", FEES,
            min_duration_ms=400, min_observations=5, min_mean_pct=0.1,
        )
        assert result is not None
        assert result.p80_pct == pytest.approx(0.9)
        assert result.max_pct == pytest.approx(1.0)

    def test_timestamps(self):
        ticks = _make_ticks(15, start_timestamp=5000, interval=100, spread_pct=0.3)
        result = evaluate_opportunity(
            ticks, "PENGU", "binance", "mexc", FEES,
            min_duration_ms=400, min_observations=10, min_mean_pct=0.1,
        )
        assert result is not None
        assert result.start_ts == 5000
        assert result.end_ts == 5000 + 14 * 100

    def test_profit_uses_p80_tick(self):
        # Create ticks where p80 tick has known quantities
        ticks = []
        for i in range(10):
            spread = 0.1 * (i + 1)
            ticks.append(_make_tick(
                1000 + i * 100, spread,
                ask_price=100.0, bid_price=101.0,
                ask_quantity=10.0 + i, bid_quantity=20.0,
            ))
        result = evaluate_opportunity(
            ticks, "PENGU", "binance", "mexc", FEES,
            min_duration_ms=400, min_observations=5, min_mean_pct=0.1,
        )
        assert result is not None
        # p80 tick (index 8 when sorted by spread) has ask_quantity=18.0, bid_quantity=20.0
        assert result.qty == pytest.approx(18.0)

    def test_unknown_exchange_default_fee(self):
        ticks = _make_ticks(20, start_timestamp=1000, interval=50, spread_pct=0.5)
        result = evaluate_opportunity(
            ticks, "PENGU", "unknown_exchange", "another_exchange", FEES,
            min_duration_ms=400, min_observations=10, min_mean_pct=0.1,
        )
        assert result is not None
        # default fee 0.001 should be used
        p80_tick = sorted(ticks, key=lambda t: t["spread_pct"])[int(len(ticks) * 0.8)]
        quantity, expected_profit = compute_profit_usd(p80_tick, 0.001, 0.001)
        assert result.profit_usdt == pytest.approx(expected_profit)


# ---------------------------------------------------------------------------
# fmt_ts
# ---------------------------------------------------------------------------

class TestFmtTs:
    def test_midnight_utc(self):
        # 2024-01-01 00:00:00.000 UTC
        assert fmt_ts(1704067200000) == "00:00:00.000"

    def test_with_millis(self):
        # 2024-01-01 00:00:01.234 UTC
        assert fmt_ts(1704067201234) == "00:00:01.234"
