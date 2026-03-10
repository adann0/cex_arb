import asyncio
import json
import logging
import time

import websockets

logger = logging.getLogger(__name__)

EXCHANGE = "binance"
WS_URL = "wss://stream.binance.com:9443/stream"


async def stream(tickers: list[str], queue: asyncio.Queue[dict[str, str | int | float]]) -> None:
    """Stream best bid/ask from Binance bookTicker WebSocket and push to queue."""
    streams = "/".join(f"{t.lower()}usdt@bookTicker" for t in tickers)

    # Reconnect loop: re-establishes WebSocket on any error
    delay = 5
    while True:
        try:
            async with websockets.connect(f"{WS_URL}?streams={streams}") as ws:
                delay = 5  # reset backoff on successful connection
                async for raw in ws:
                    data = json.loads(raw)["data"]
                    await queue.put({
                        "timestamp":    int(time.time() * 1000),
                        "exchange":     EXCHANGE,
                        "ticker":       data["s"].removesuffix("USDT"),
                        "bid_price":    float(data["b"]),
                        "bid_quantity": float(data["B"]),
                        "ask_price":    float(data["a"]),
                        "ask_quantity": float(data["A"]),
                    })
        except Exception as e:
            logger.warning("%s -reconnecting in %ds", e, delay)
            await asyncio.sleep(delay)
            delay = min(delay * 2, 60)