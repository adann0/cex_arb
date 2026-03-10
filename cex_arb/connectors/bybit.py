import asyncio
import json
import logging

import websockets

logger = logging.getLogger(__name__)

EXCHANGE = "bybit"
WS_URL = "wss://stream.bybit.com/v5/public/spot"


async def stream(tickers: list[str], queue: asyncio.Queue[dict[str, str | int | float]]) -> None:
    """Stream best bid/ask from Bybit level-1 orderbook WebSocket and push to queue."""
    args = [f"orderbook.1.{t}USDT" for t in tickers]

    # Reconnect loop: re-establishes WebSocket on any error
    delay = 5
    while True:
        try:
            async with websockets.connect(WS_URL) as ws:
                delay = 5  # reset backoff on successful connection
                await ws.send(json.dumps({"op": "subscribe", "args": args}))

                async for raw in ws:
                    msg = json.loads(raw)
                    if msg.get("op"):
                        continue

                    data = msg.get("data", {})
                    bids = data.get("b", [])
                    asks = data.get("a", [])
                    if not bids or not asks:
                        continue

                    await queue.put({
                        "timestamp":    int(msg["ts"]),
                        "exchange":     EXCHANGE,
                        "ticker":       data["s"].removesuffix("USDT"),
                        "bid_price":    float(bids[0][0]),
                        "bid_quantity": float(bids[0][1]),
                        "ask_price":    float(asks[0][0]),
                        "ask_quantity": float(asks[0][1]),
                    })

        except Exception as e:
            logger.warning("%s -reconnecting in %ds", e, delay)
            await asyncio.sleep(delay)
            delay = min(delay * 2, 60)