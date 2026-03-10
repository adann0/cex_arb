import asyncio
import json
import logging

import websockets

logger = logging.getLogger(__name__)

EXCHANGE = "mexc"
WS_URL = "wss://wbs-api.mexc.com/ws"
PING_INTERVAL = 20


# ---------------------------------------------------------------------------
# Minimal protobuf parser -no dependencies
# ---------------------------------------------------------------------------

def _read_varint(data: bytes, pos: int) -> tuple[int, int]:
    """Decode a protobuf varint starting at pos. Return (value, new position)."""
    result, shift = 0, 0
    while True:
        b = data[pos]; pos += 1
        result |= (b & 0x7F) << shift
        if not (b & 0x80):
            break
        shift += 7
    return result, pos


def _parse_proto(data: bytes) -> dict:
    """Parse a flat protobuf message into a {field_number: value} dict."""
    pos = 0
    fields: dict = {}
    while pos < len(data):
        tag, pos = _read_varint(data, pos)
        field_num = tag >> 3
        wire_type = tag & 0x7
        if wire_type == 0:                      # varint
            val, pos = _read_varint(data, pos)
            fields[field_num] = val
        elif wire_type == 2:                    # length-delimited
            length, pos = _read_varint(data, pos)
            raw = data[pos:pos + length]; pos += length
            try:
                fields[field_num] = raw.decode("utf-8")
            except UnicodeDecodeError:
                fields[field_num] = raw         # nested proto, keep as bytes
        else:
            break                               # unknown wire type, stop
    return fields


def _decode(frame: bytes) -> dict | None:
    """
    Top-level proto fields:
      1  = channel string
      3  = symbol  (e.g. "BTCUSDT")
      6  = timestamp ms (varint)
      315 = nested bookTicker bytes:
              1 = bidPrice, 2 = bidQty, 3 = askPrice, 4 = askQty
    """
    top = _parse_proto(frame)
    if 315 not in top or 3 not in top or 6 not in top:
        return None
    nested_raw = top[315]
    if isinstance(nested_raw, str):
        nested_raw = nested_raw.encode("latin-1")
    inner = _parse_proto(nested_raw)
    if not all(k in inner for k in (1, 2, 3, 4)):
        return None
    symbol: str = top[3]
    return {
        "timestamp":    top[6],
        "exchange":     EXCHANGE,
        "ticker":       symbol.removesuffix("USDT"),
        "bid_price":    float(inner[1]),
        "bid_quantity": float(inner[2]),
        "ask_price":    float(inner[3]),
        "ask_quantity": float(inner[4]),
    }


# ---------------------------------------------------------------------------
# Stream
# ---------------------------------------------------------------------------

async def stream(tickers: list[str], queue: asyncio.Queue[dict[str, str | int | float]]) -> None:
    """Stream best bid/ask from Mexc protobuf WebSocket and push to queue."""
    params = [
        f"spot@public.aggre.bookTicker.v3.api.pb@100ms@{t}USDT"
        for t in tickers
    ]

    # Reconnect loop: re-establishes WebSocket on any error
    delay = 5
    while True:
        try:
            async with websockets.connect(WS_URL) as ws:
                delay = 5  # reset backoff on successful connection
                await ws.send(json.dumps({"method": "SUBSCRIPTION", "params": params}))

                async def _ping():
                    while True:
                        await asyncio.sleep(PING_INTERVAL)
                        await ws.send(json.dumps({"method": "PING"}))

                ping_task = asyncio.create_task(_ping())
                try:
                    async for frame in ws:
                        if isinstance(frame, str):
                            continue        # subscription ack / pong -ignore
                        record = _decode(frame)
                        if record:
                            await queue.put(record)
                finally:
                    ping_task.cancel()

        except Exception as e:
            logger.warning("%s -reconnecting in %ds", e, delay)
            await asyncio.sleep(delay)
            delay = min(delay * 2, 60)