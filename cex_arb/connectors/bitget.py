import asyncio
import json
import logging

import websockets

logger = logging.getLogger(__name__)

EXCHANGE = "bitget"
WS_URL = "wss://ws.bitget.com/v2/ws/public"


async def stream(tickers: list[str], queue: asyncio.Queue[dict[str, str | int | float]]) -> None:
    """Stream best bid/ask from Bitget ticker WebSocket and push to queue."""
    args = [{"instType": "SPOT", "channel": "ticker", "instId": f"{t}USDT"} for t in tickers]

    # Reconnect loop: re-establishes WebSocket on any error
    delay = 5
    while True:
        try:
            async with websockets.connect(
                WS_URL,
                ping_interval=10,
                ping_timeout=5
            ) as ws:
                delay = 5  # reset backoff on successful connection
                await ws.send(json.dumps({"op": "subscribe", "args": args}))

                async for raw in ws:

                    if raw in ("ping", "pong"):
                        continue

                    msg = json.loads(raw)
                    if msg.get("event"):  # skip subscribe ack
                        continue

                    for data in msg.get("data", []):
                        if not data.get("bidPr"):
                            continue
                        await queue.put({
                            "timestamp":    int(data["ts"]),
                            "exchange":     EXCHANGE,
                            "ticker":       data["instId"].removesuffix("USDT"),
                            "bid_price":    float(data["bidPr"]),
                            "bid_quantity": float(data["bidSz"]),
                            "ask_price":    float(data["askPr"]),
                            "ask_quantity": float(data["askSz"]),
                        })

        except Exception as e:
            logger.warning("%s -reconnecting in %ds", e, delay)
            await asyncio.sleep(delay)
            delay = min(delay * 2, 60)