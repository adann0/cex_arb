#!/usr/bin/env python3

import json
import logging
from collections import namedtuple
from dataclasses import asdict, dataclass
from datetime import datetime, timezone

from sqlalchemy import create_engine, text

from cex_arb.models import Base

logger = logging.getLogger(__name__)

type OpportunityKey = tuple[str, str, str]  # (ticker, ask_exchange, bid_exchange)
type QuoteLevel = dict[str, float]      # {bid_price, bid_quantity, ask_price, ask_quantity}
type ExchangeBook = dict[str, QuoteLevel]  # maps exchange name to its best bid/ask snapshot
type TickSnapshot = dict[str, float]    # {timestamp, spread_pct, bid/ask price+quantity}

TICKER = 'PENGU'

DB_URL = "sqlite:///db.sql"

# Taker fees as a decimal fraction (e.g. 0.0010 = 0.10%)
FEES = {
    "binance":  0.0010,
    "coinbase": 0.0060,  # retail, < $10k/month
    "upbit":    0.0025,
    "okx":      0.0010,
    "bybit":    0.0010,
    "bitget":   0.0010,
    "gate":     0.0010,
    "kucoin":   0.0010,
    "mexc":     0.0005,
}

MIN_DURATION_MS = 400
MIN_OBS = 10
COOLDOWN_MS = 10000
# Minimum viable mean spread %: roughly 1/3 goes to the ask exchange fee, 1/3 to the
# bid exchange fee, 1/3 is actual profit. Below this threshold the risk outweighs the reward.
MIN_MEAN_PCT = 0.1

engine = create_engine(DB_URL, echo=False)


# -- Helpers -----------------------------------------------------------------

Tick = namedtuple("Tick", ["timestamp", "exchange", "ticker", "bid_price", "bid_quantity", "ask_price", "ask_quantity"])

_TICK_QUERY = text("""
    SELECT timestamp, exchange, ticker, bid_price, bid_quantity, ask_price, ask_quantity
    FROM order_book
    WHERE ticker = :ticker
    ORDER BY timestamp, id
""")


def load_ticks(session, ticker: str) -> list[Tick]:
    """Load all order book rows for a ticker as lightweight named tuples.

    Uses raw SQL instead of the ORM to avoid per-row object overhead.
    """
    rows = session.execute(_TICK_QUERY, {"ticker": ticker}).fetchall()
    return [Tick(*row) for row in rows]


def fmt_ts(ms: int) -> str:
    """Format a millisecond UTC timestamp as HH:MM:SS.mmm."""
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime("%H:%M:%S.%f")[:-3]


# -- Pure functions ----------------------------------------------------------

@dataclass
class OpportunityResult:
    ticker: str
    ask_exchange: str
    bid_exchange: str
    start_ts: int
    end_ts: int
    duration_ms: int
    mean_pct: float
    max_pct: float
    p80_pct: float
    obs: int
    qty: float
    profit_usdt: float


def compute_net_spread_pct(bid_price: float, ask_price: float, bid_fee: float, ask_fee: float) -> float:
    """Return the net spread percentage after taker fees on both sides."""
    return (bid_price * (1 - bid_fee) / (ask_price * (1 + ask_fee)) - 1) * 100


def find_best_exchanges(book: ExchangeBook) -> tuple[str, str]:
    """Return (lowest ask exchange, highest bid exchange) from the book."""
    best_ask_exchange = min(book, key=lambda exchange: book[exchange]["ask_price"])
    best_bid_exchange = max(book, key=lambda exchange: book[exchange]["bid_price"])
    return best_ask_exchange, best_bid_exchange


def compute_profit_usd(tick: TickSnapshot, ask_fee: float, bid_fee: float) -> tuple[float, float]:
    """Return (tradeable quantity, profit in USDT) for a single tick snapshot."""
    quantity = min(tick["ask_quantity"], tick["bid_quantity"])
    usdt_spent = quantity * tick["ask_price"] * (1 + ask_fee)
    usdt_received = quantity * tick["bid_price"] * (1 - bid_fee)
    profit_usdt = usdt_received - usdt_spent
    return quantity, profit_usdt


def evaluate_opportunity(
    ticks: list[TickSnapshot],
    ticker: str,
    ask_exchange: str,
    bid_exchange: str,
    fees: dict[str, float],
    min_duration_ms: int,
    min_observations: int,
    min_mean_pct: float,
) -> OpportunityResult | None:
    """Evaluate an opportunity window and return an OpportunityResult if it passes all filters."""
    start_timestamp = ticks[0]["timestamp"]
    end_timestamp = ticks[-1]["timestamp"]
    duration = end_timestamp - start_timestamp
    if duration < min_duration_ms or len(ticks) < min_observations:
        return None
    spread_percentages = [tick["spread_pct"] for tick in ticks]
    mean_pct = sum(spread_percentages) / len(spread_percentages)
    if mean_pct < min_mean_pct:
        return None
    sorted_ticks = sorted(ticks, key=lambda tick: tick["spread_pct"])
    p80_index = int(len(sorted_ticks) * 0.8)
    p80_tick = sorted_ticks[p80_index]
    p80_pct = p80_tick["spread_pct"]
    ask_fee = fees.get(ask_exchange, 0.001)
    bid_fee = fees.get(bid_exchange, 0.001)
    quantity, profit_usdt = compute_profit_usd(p80_tick, ask_fee, bid_fee)
    return OpportunityResult(
        ticker=ticker,
        ask_exchange=ask_exchange,
        bid_exchange=bid_exchange,
        start_ts=start_timestamp,
        end_ts=end_timestamp,
        duration_ms=duration,
        mean_pct=mean_pct,
        max_pct=max(spread_percentages),
        p80_pct=p80_pct,
        obs=len(ticks),
        qty=quantity,
        profit_usdt=profit_usdt,
    )


# -- Main --------------------------------------------------------------------

def main() -> list[OpportunityResult]:
    """Replay all ticks for TICKER, detect arbitrage opportunities, and write results to backtest.json."""
    total_profit: float = 0
    results: list[OpportunityResult] = []

    order_book: dict[str, ExchangeBook] = {}              # maps ticker to its exchange book
    open_opportunities: dict[OpportunityKey, list[TickSnapshot]] = {}      # maps opportunity key to its accumulated ticks

    # cooldown[exchange] = timestamp ms until which the exchange is unavailable
    cooldown: dict[str, int] = {}

    def close_opportunity(key: OpportunityKey, ticks: list[TickSnapshot]) -> None:
        """Evaluate and record an opportunity, then apply cooldown to both exchanges."""
        nonlocal total_profit
        ticker, ask_exchange, bid_exchange = key
        result = evaluate_opportunity(
            ticks, ticker, ask_exchange, bid_exchange, FEES,
            MIN_DURATION_MS, MIN_OBS, MIN_MEAN_PCT,
        )
        if result is None:
            return
        total_profit += result.profit_usdt
        # Mark both exchanges on cooldown from the start of the opportunity
        cooldown[ask_exchange] = result.start_ts + COOLDOWN_MS
        cooldown[bid_exchange] = result.start_ts + COOLDOWN_MS
        # Close any other open opportunities that share a now-cooling exchange
        for other_key in list(open_opportunities):
            _, other_ask, other_bid = other_key
            if other_ask in (ask_exchange, bid_exchange) or other_bid in (ask_exchange, bid_exchange):
                open_opportunities.pop(other_key)  # discard, contaminated by same exchange
        results.append(result)
        logger.info(
            "[%s] %6s dur=%5dms | mean=%+.4f%% | max=%+.4f%% | "
            "p80=%+.4f%% | obs=%4d | qty=%.2f | profit=%+.2f USDT | "
            "total_profit=%+.2f USDT | buy @ %-8s -> sell @ %s",
            fmt_ts(result.start_ts), result.ticker, result.duration_ms,
            result.mean_pct, result.max_pct, result.p80_pct, result.obs,
            result.qty, result.profit_usdt, total_profit,
            result.ask_exchange, result.bid_exchange,
        )

    with engine.connect() as connection:
        all_ticks = load_ticks(connection, TICKER)

    for tick in all_ticks:
        ticker = tick.ticker
        exchange = tick.exchange

        if ticker not in order_book:
            order_book[ticker] = {}

        order_book[ticker][exchange] = {
            "bid_price": tick.bid_price,
            "bid_quantity": tick.bid_quantity,
            "ask_price": tick.ask_price,
            "ask_quantity": tick.ask_quantity,
        }

        book = order_book[ticker]
        if len(book) < 2:
            continue

        best_ask_exchange, best_bid_exchange = find_best_exchanges(book)

        if best_bid_exchange == best_ask_exchange:
            continue

        best_bid = book[best_bid_exchange]["bid_price"]
        best_ask = book[best_ask_exchange]["ask_price"]
        ask_fee = FEES.get(best_ask_exchange, 0.001)
        bid_fee = FEES.get(best_bid_exchange, 0.001)
        spread_pct = compute_net_spread_pct(best_bid, best_ask, bid_fee, ask_fee)

        key = (ticker, best_ask_exchange, best_bid_exchange)
        current_timestamp = tick.timestamp

        if spread_pct > 0:
            # If either exchange is on cooldown, skip entirely
            if cooldown.get(best_ask_exchange, 0) > current_timestamp or cooldown.get(best_bid_exchange, 0) > current_timestamp:
                # Close any opportunity that was open for these exchanges
                if key in open_opportunities:
                    close_opportunity(key, open_opportunities.pop(key))
                continue

            if key not in open_opportunities:
                # Close any other open opportunity on this ticker with a different exchange pair
                for other_key in list(open_opportunities):
                    if other_key[0] == ticker and other_key != key:
                        close_opportunity(other_key, open_opportunities.pop(other_key))
                open_opportunities[key] = []

            open_opportunities[key].append({
                "timestamp": current_timestamp,
                "spread_pct": spread_pct,
                "ask_price": best_ask,
                "ask_quantity": book[best_ask_exchange]["ask_quantity"],
                "bid_price": best_bid,
                "bid_quantity": book[best_bid_exchange]["bid_quantity"],
            })

        else:
            if key in open_opportunities:
                close_opportunity(key, open_opportunities.pop(key))

    # Flush any still-open opportunities at end of data
    for key, ticks in list(open_opportunities.items()):
        close_opportunity(key, ticks)

    with open("backtest.json", "w") as f:
        json.dump([asdict(result) for result in results], f, indent=2)

    return results


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    main()