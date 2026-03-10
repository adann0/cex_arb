import asyncio
import json
import logging
import uuid

import aiohttp
import websockets

logger = logging.getLogger(__name__)

EXCHANGE = "kucoin"
TOKEN_URL = "https://api.kucoin.com/api/v1/bullet-public"
PING_INTERVAL = 18  # seconds


async def _get_ws_url() -> str:
    """Fetch a temporary WebSocket token from Kucoin and build the connection URL."""
    async with aiohttp.ClientSession() as session:
        async with session.post(TOKEN_URL) as resp:
            data = await resp.json()
    token = data["data"]["token"]
    endpoint = data["data"]["instanceServers"][0]["endpoint"]
    return f"{endpoint}?token={token}&connectId={uuid.uuid4()}"


async def stream(tickers: list[str], queue: asyncio.Queue[dict[str, str | int | float]]) -> None:
    """Stream best bid/ask from Kucoin ticker WebSocket and push to queue."""
    symbols = ",".join(f"{t}-USDT" for t in tickers)
    topic = f"/market/ticker:{symbols}"

    # Reconnect loop: re-establishes WebSocket on any error
    delay = 5
    while True:
        try:
            url = await _get_ws_url()
            async with websockets.connect(url) as ws:
                delay = 5  # reset backoff on successful connection
                # wait for welcome message
                await ws.recv()

                await ws.send(json.dumps({
                    "id":                str(uuid.uuid4()),
                    "type":              "subscribe",
                    "topic":             topic,
                    "privateChannel":    False,
                    "response":          True,
                }))

                async def _ping():
                    while True:
                        await asyncio.sleep(PING_INTERVAL)
                        await ws.send(json.dumps({"id": str(uuid.uuid4()), "type": "ping"}))

                ping_task = asyncio.create_task(_ping())
                try:
                    async for raw in ws:
                        msg = json.loads(raw)
                        if msg.get("type") != "message":
                            continue

                        data = msg.get("data", {})
                        if not data.get("bestBid"):
                            continue

                        await queue.put({
                            "timestamp":    int(data["time"]),
                            "exchange":     EXCHANGE,
                            "ticker":       msg["topic"].split(":")[-1].removesuffix("-USDT"),
                            "bid_price":    float(data["bestBid"]),
                            "bid_quantity": float(data["bestBidSize"]),
                            "ask_price":    float(data["bestAsk"]),
                            "ask_quantity": float(data["bestAskSize"]),
                        })
                finally:
                    ping_task.cancel()

        except Exception as e:
            logger.warning("%s -reconnecting in %ds", e, delay)
            await asyncio.sleep(delay)
            delay = min(delay * 2, 60)