"""
Functional tests for WebSocket connectors.

Each test launches a connector, waits for at least one valid order book message,
then cancels the task. Fails after 60 seconds with no message received.

These tests hit real exchange WebSocket endpoints and require internet access.
Run with: pytest tests/test_connectors_live.py -v
"""

import asyncio

import pytest

from cex_arb.connectors import binance, coinbase, okx, bybit, upbit, kucoin, mexc, gate, bitget

TICKERS = ["BTC"]
# Some exchanges have low volume tickers, so allow enough time to avoid false negatives
TIMEOUT_SECONDS = 60
EXPECTED_KEYS = {"timestamp", "exchange", "ticker", "bid_price", "bid_quantity", "ask_price", "ask_quantity"}


async def _receive_one_tick(connector_module) -> dict:
    """Launch a connector, wait for one message, cancel, and return it."""
    queue: asyncio.Queue = asyncio.Queue()
    task = asyncio.create_task(connector_module.stream(TICKERS, queue))
    try:
        record = await asyncio.wait_for(queue.get(), timeout=TIMEOUT_SECONDS)
        return record
    finally:
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass


def _validate_record(record: dict, expected_exchange: str) -> None:
    """Assert that a tick record has the correct structure and values."""
    assert set(record.keys()) == EXPECTED_KEYS
    assert record["exchange"] == expected_exchange
    assert record["ticker"] == "BTC"
    assert isinstance(record["timestamp"], (int, float))
    assert record["bid_price"] > 0
    assert record["bid_quantity"] > 0
    assert record["ask_price"] > 0
    assert record["ask_quantity"] > 0
    assert record["bid_price"] <= record["ask_price"]


@pytest.mark.asyncio
async def test_binance():
    """Receive at least one tick from Binance."""
    record = await _receive_one_tick(binance)
    _validate_record(record, "binance")


@pytest.mark.asyncio
async def test_coinbase():
    """Receive at least one tick from Coinbase."""
    record = await _receive_one_tick(coinbase)
    _validate_record(record, "coinbase")


@pytest.mark.asyncio
async def test_okx():
    """Receive at least one tick from OKX."""
    record = await _receive_one_tick(okx)
    _validate_record(record, "okx")


@pytest.mark.asyncio
async def test_bybit():
    """Receive at least one tick from Bybit."""
    record = await _receive_one_tick(bybit)
    _validate_record(record, "bybit")


@pytest.mark.asyncio
async def test_upbit():
    """Receive at least one tick from Upbit."""
    record = await _receive_one_tick(upbit)
    _validate_record(record, "upbit")


@pytest.mark.asyncio
async def test_kucoin():
    """Receive at least one tick from Kucoin."""
    record = await _receive_one_tick(kucoin)
    _validate_record(record, "kucoin")


@pytest.mark.asyncio
async def test_mexc():
    """Receive at least one tick from Mexc."""
    record = await _receive_one_tick(mexc)
    _validate_record(record, "mexc")


@pytest.mark.asyncio
async def test_gate():
    """Receive at least one tick from Gate.io."""
    record = await _receive_one_tick(gate)
    _validate_record(record, "gate")


@pytest.mark.asyncio
async def test_bitget():
    """Receive at least one tick from Bitget."""
    record = await _receive_one_tick(bitget)
    _validate_record(record, "bitget")
