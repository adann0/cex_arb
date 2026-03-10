import asyncio
import json
import logging
import time

import websockets

logger = logging.getLogger(__name__)

EXCHANGE = "coinbase"
WS_URL = "wss://advanced-trade-ws.coinbase.com"


async def stream(tickers: list[str], queue: asyncio.Queue[dict[str, str | int | float]]) -> None:
    """Stream best bid/ask from Coinbase Advanced Trade WebSocket and push to queue."""
    product_ids = [f"{t}-USD" for t in tickers]

    # Reconnect loop: re-establishes WebSocket on any error
    delay = 5
    while True:
        try:
            async with websockets.connect(WS_URL) as ws:
                delay = 5  # reset backoff on successful connection
                await ws.send(json.dumps({
                    "type":        "subscribe",
                    "product_ids": product_ids,
                    "channel":     "ticker",
                }))

                async for raw in ws:
                    msg = json.loads(raw)
                    if msg.get("channel") != "ticker":
                        continue

                    for event in msg.get("events", []):
                        for ticker in event.get("tickers", []):
                            if not ticker.get("best_bid"):
                                continue
                            await queue.put({
                                "timestamp":    int(time.time() * 1000),
                                "exchange":     EXCHANGE,
                                "ticker":       ticker["product_id"].removesuffix("-USD"),
                                "bid_price":    float(ticker["best_bid"]),
                                "bid_quantity": float(ticker["best_bid_quantity"]),
                                "ask_price":    float(ticker["best_ask"]),
                                "ask_quantity": float(ticker["best_ask_quantity"]),
                            })

        except Exception as e:
            logger.warning("%s -reconnecting in %ds", e, delay)
            await asyncio.sleep(delay)
            delay = min(delay * 2, 60)