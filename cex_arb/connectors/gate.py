import asyncio
import json
import logging
import time

import websockets

logger = logging.getLogger(__name__)

EXCHANGE = "gate"
WS_URL = "wss://api.gateio.ws/ws/v4/"


async def stream(tickers: list[str], queue: asyncio.Queue[dict[str, str | int | float]]) -> None:
    """Stream best bid/ask from Gate.io book ticker WebSocket and push to queue."""
    pairs = [f"{t}_USDT" for t in tickers]

    # Reconnect loop: re-establishes WebSocket on any error
    delay = 5
    while True:
        try:
            async with websockets.connect(WS_URL) as ws:
                delay = 5  # reset backoff on successful connection
                for pair in pairs:
                    await ws.send(json.dumps({
                        "time":    int(time.time()),
                        "channel": "spot.book_ticker",
                        "event":   "subscribe",
                        "payload": [pair],
                    }))

                async for raw in ws:
                    msg = json.loads(raw)
                    if msg.get("event") != "update":
                        continue

                    result = msg.get("result", {})
                    if not result.get("b") or not result.get("a"):
                        continue

                    await queue.put({
                        "timestamp":    int(result["t"]),
                        "exchange":     EXCHANGE,
                        "ticker":       result["s"].removesuffix("_USDT"),
                        "bid_price":    float(result["b"]),
                        "bid_quantity": float(result["B"]),
                        "ask_price":    float(result["a"]),
                        "ask_quantity": float(result["A"]),
                    })

        except Exception as e:
            logger.warning("%s -reconnecting in %ds", e, delay)
            await asyncio.sleep(delay)
            delay = min(delay * 2, 60)