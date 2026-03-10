import asyncio
import json
import logging

import websockets

logger = logging.getLogger(__name__)

EXCHANGE = "okx"
WS_URL = "wss://ws.okx.com:8443/ws/v5/public"


async def stream(tickers: list[str], queue: asyncio.Queue[dict[str, str | int | float]]) -> None:
    """Stream best bid/ask from OKX tickers WebSocket and push to queue."""
    args = [{"channel": "tickers", "instId": f"{t}-USDT"} for t in tickers]

    # Reconnect loop: re-establishes WebSocket on any error
    delay = 5
    while True:
        try:
            async with websockets.connect(WS_URL) as ws:
                delay = 5  # reset backoff on successful connection
                await ws.send(json.dumps({"op": "subscribe", "args": args}))

                async for raw in ws:
                    msg = json.loads(raw)
                    if msg.get("event"):  # skip subscribe ack
                        continue

                    for data in msg.get("data", []):
                        if not data.get("bidPx"):
                            continue
                        await queue.put({
                            "timestamp":    int(data["ts"]),
                            "exchange":     EXCHANGE,
                            "ticker":       data["instId"].removesuffix("-USDT"),
                            "bid_price":    float(data["bidPx"]),
                            "bid_quantity": float(data["bidSz"]),
                            "ask_price":    float(data["askPx"]),
                            "ask_quantity": float(data["askSz"]),
                        })

        except Exception as e:
            logger.warning("%s -reconnecting in %ds", e, delay)
            await asyncio.sleep(delay)
            delay = min(delay * 2, 60)