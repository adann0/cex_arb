"""
End-to-end functional test for the full collection pipeline.

Launches all 9 WebSocket connectors and the database writer against a temporary
SQLite database. Polls the database until every exchange has at least one row,
then tears everything down. Fails after 60 seconds.

Run with: pytest tests/test_pipeline_live.py -v
"""

import asyncio

import pytest
from sqlalchemy import create_engine, event, text
from sqlalchemy.orm import sessionmaker

from cex_arb.connectors import binance, coinbase, okx, bybit, upbit, kucoin, mexc, gate, bitget
from cex_arb.models import Base, OrderBook

CONNECTORS = [binance, coinbase, okx, bybit, upbit, kucoin, mexc, gate, bitget]
EXPECTED_EXCHANGES = {"binance", "coinbase", "okx", "bybit", "upbit", "kucoin", "mexc", "gate", "bitget"}
TICKERS = ["BTC"]
# Some exchanges have low volume tickers, so allow enough time to avoid false negatives
TIMEOUT_SECONDS = 60
POLL_INTERVAL_SECONDS = 0.5


async def _db_writer_immediate(queue: asyncio.Queue, session_factory: sessionmaker) -> None:
    """Write every record to the database immediately, no batching."""
    with session_factory() as session:
        try:
            while True:
                record = await queue.get()
                session.bulk_insert_mappings(OrderBook, [record])
                session.commit()
        except asyncio.CancelledError:
            pass


async def _wait_for_all_exchanges(engine, timeout: float) -> set[str]:
    """Poll the database until all expected exchanges have at least one row."""
    deadline = asyncio.get_event_loop().time() + timeout
    while asyncio.get_event_loop().time() < deadline:
        with engine.connect() as connection:
            rows = connection.execute(text("SELECT DISTINCT exchange FROM order_book")).fetchall()
        found = {row[0] for row in rows}
        if EXPECTED_EXCHANGES.issubset(found):
            return found
        await asyncio.sleep(POLL_INTERVAL_SECONDS)
    with engine.connect() as connection:
        rows = connection.execute(text("SELECT DISTINCT exchange FROM order_book")).fetchall()
    return {row[0] for row in rows}


@pytest.mark.asyncio
async def test_full_pipeline(tmp_path):
    """All 9 connectors write at least one row each to a temporary database."""
    db_path = tmp_path / "test.db"
    db_url = f"sqlite:///{db_path}"

    engine = create_engine(db_url, echo=False, future=True, connect_args={"timeout": 30})

    @event.listens_for(engine, "connect")
    def set_pragmas(dbapi_conn, _):
        cursor = dbapi_conn.cursor()
        cursor.execute("PRAGMA journal_mode=WAL;")
        cursor.execute("PRAGMA synchronous=NORMAL;")
        cursor.close()

    Base.metadata.create_all(engine)
    session_factory = sessionmaker(bind=engine, expire_on_commit=False)

    queue: asyncio.Queue = asyncio.Queue(maxsize=50_000)

    connector_tasks = [asyncio.create_task(connector.stream(TICKERS, queue)) for connector in CONNECTORS]
    writer_task = asyncio.create_task(_db_writer_immediate(queue, session_factory))

    try:
        found_exchanges = await _wait_for_all_exchanges(engine, TIMEOUT_SECONDS)
        missing = EXPECTED_EXCHANGES - found_exchanges
        assert not missing, f"Timed out waiting for exchanges: {missing}"
    finally:
        for task in connector_tasks:
            task.cancel()
        writer_task.cancel()
        await asyncio.gather(*connector_tasks, writer_task, return_exceptions=True)
        engine.dispose()

    verify_engine = create_engine(db_url)
    with verify_engine.connect() as connection:
        total_rows = connection.execute(text("SELECT COUNT(*) FROM order_book")).scalar()
        exchange_counts = connection.execute(
            text("SELECT exchange, COUNT(*) FROM order_book GROUP BY exchange ORDER BY exchange")
        ).fetchall()
    verify_engine.dispose()

    assert total_rows >= 9
    for exchange_name, count in exchange_counts:
        assert count >= 1, f"{exchange_name} has 0 rows"
