import asyncio
import logging
import pathlib
from types import ModuleType

import yaml

from cex_arb.models import OrderBook, SessionLocal, init_db
from cex_arb.connectors import binance, coinbase, upbit, okx, bybit, bitget, gate, kucoin, mexc

logger = logging.getLogger(__name__)

type TickRecord = dict[str, str | int | float]  # {timestamp, exchange, ticker, bid/ask price+quantity}

CONFIG_PATH = pathlib.Path(__file__).parent.parent / "config.yaml"
CONNECTORS: list[ModuleType] = [binance, coinbase, upbit, okx, bybit, bitget, gate, kucoin, mexc]
BATCH_SIZE = 1000



def load_tickers() -> list[str]:
    """Read ticker symbols from config.yaml and return them uppercased."""
    cfg = yaml.safe_load(CONFIG_PATH.read_text())
    return [str(t).upper() for t in cfg["tickers"]]


async def _db_writer(queue: asyncio.Queue[TickRecord]) -> None:
    """Consume tick records from the queue and batch-insert them into SQLite."""
    buffer: list[TickRecord] = []
    written: int = 0
    with SessionLocal() as session:
        try:
            while True:
                buffer.append(await queue.get())
                if len(buffer) >= BATCH_SIZE:
                    session.bulk_insert_mappings(OrderBook, buffer)
                    session.commit()
                    written += BATCH_SIZE
                    buffer.clear()
                    logger.info("written=%s buffered=%d qsize=%d", f"{written:,}", len(buffer), queue.qsize())
        finally:
            if buffer:
                session.bulk_insert_mappings(OrderBook, buffer)
                session.commit()
                written += len(buffer)
                logger.info("flushed remaining %d rows (total=%s)", len(buffer), f"{written:,}")


async def main() -> None:
    """Launch all exchange WebSocket connectors and the database writer."""
    init_db()
    tickers = load_tickers()
    queue: asyncio.Queue[TickRecord] = asyncio.Queue(maxsize=50_000)

    tasks = [asyncio.create_task(c.stream(tickers, queue)) for c in CONNECTORS]
    tasks.append(asyncio.create_task(_db_writer(queue)))

    try:
        await asyncio.gather(*tasks)
    except asyncio.CancelledError:
        pass


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("stopped")