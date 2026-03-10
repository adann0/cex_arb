import asyncio
import json
import logging
import uuid

import websockets

logger = logging.getLogger(__name__)

EXCHANGE = "upbit"
WS_URL = "wss://api.upbit.com/websocket/v1"


async def stream(tickers: list[str], queue: asyncio.Queue[dict[str, str | int | float]]) -> None:
    """Stream best bid/ask from Upbit orderbook WebSocket and push to queue."""
    codes = [f"USDT-{t}" for t in tickers]

    # Reconnect loop: re-establishes WebSocket on any error
    delay = 5
    while True:
        try:
            async with websockets.connect(WS_URL) as ws:
                delay = 5  # reset backoff on successful connection
                await ws.send(json.dumps([
                    {"ticket": str(uuid.uuid4())},
                    {"type": "orderbook", "codes": codes},
                ]))

                async for raw in ws:
                    data = json.loads(raw)
                    if data.get("type") != "orderbook":
                        continue

                    top = data["orderbook_units"][0]
                    await queue.put({
                        "timestamp":    data["timestamp"],
                        "exchange":     EXCHANGE,
                        "ticker":       data["code"].removeprefix("USDT-"),
                        "bid_price":    top["bid_price"],
                        "bid_quantity": top["bid_size"],
                        "ask_price":    top["ask_price"],
                        "ask_quantity": top["ask_size"],
                    })

        except Exception as e:
            logger.warning("%s -reconnecting in %ds", e, delay)
            await asyncio.sleep(delay)
            delay = min(delay * 2, 60)