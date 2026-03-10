import pytest
from cex_arb.connectors.mexc import _read_varint, _parse_proto, _decode


# ---------------------------------------------------------------------------
# Protobuf encoding helpers (inverse of the parser under test)
# ---------------------------------------------------------------------------

def _encode_varint(value: int) -> bytes:
    result = []
    while value > 0x7F:
        result.append((value & 0x7F) | 0x80)
        value >>= 7
    result.append(value)
    return bytes(result)


def _encode_field_varint(field_num: int, value: int) -> bytes:
    tag = (field_num << 3) | 0
    return _encode_varint(tag) + _encode_varint(value)


def _encode_field_bytes(field_num: int, data: bytes) -> bytes:
    tag = (field_num << 3) | 2
    return _encode_varint(tag) + _encode_varint(len(data)) + data


def _encode_field_string(field_num: int, s: str) -> bytes:
    return _encode_field_bytes(field_num, s.encode("utf-8"))


def _build_inner(bid_price="0.123", bid_qty="100.0", ask_price="0.124", ask_qty="200.0") -> bytes:
    """Encode the nested bookTicker proto (fields 1-4, all strings)."""
    return (
        _encode_field_string(1, bid_price)
        + _encode_field_string(2, bid_qty)
        + _encode_field_string(3, ask_price)
        + _encode_field_string(4, ask_qty)
    )


def _build_frame(symbol="PENGUUSDT", timestamp=1700000000000, inner=None, **extra_fields) -> bytes:
    """Encode a top-level proto frame like MEXC sends."""
    if inner is None:
        inner = _build_inner()
    frame = (
        _encode_field_string(1, "spot@public.aggre.bookTicker")
        + _encode_field_string(3, symbol)
        + _encode_field_varint(6, timestamp)
        + _encode_field_bytes(315, inner)
    )
    for field_num, value in extra_fields.items():
        frame += _encode_field_varint(int(field_num), value)
    return frame


# ---------------------------------------------------------------------------
# _read_varint
# ---------------------------------------------------------------------------

class TestReadVarint:
    def test_single_byte(self):
        # Values 0-127 are encoded in one byte
        val, pos = _read_varint(bytes([42]), 0)
        assert val == 42
        assert pos == 1

    def test_zero(self):
        val, pos = _read_varint(bytes([0]), 0)
        assert val == 0
        assert pos == 1

    def test_max_single_byte(self):
        val, pos = _read_varint(bytes([127]), 0)
        assert val == 127
        assert pos == 1

    def test_two_bytes(self):
        # 300 = 0b100101100, split into 7-bit groups: 0101100 (44), 0000010 (2)
        # encoded: (44 | 0x80), 2 = 0xAC, 0x02
        val, pos = _read_varint(bytes([0xAC, 0x02]), 0)
        assert val == 300
        assert pos == 2

    def test_large_value(self):
        # Encode a known large value via our helper, then decode
        timestamp = 1700000000000
        encoded = _encode_varint(timestamp)
        val, pos = _read_varint(encoded, 0)
        assert val == timestamp
        assert pos == len(encoded)

    def test_offset(self):
        # Reading from a non-zero position
        data = bytes([0xFF, 0xFF, 42])  # junk, junk, then varint 42
        val, pos = _read_varint(data, 2)
        assert val == 42
        assert pos == 3


# ---------------------------------------------------------------------------
# _parse_proto
# ---------------------------------------------------------------------------

class TestParseProto:
    def test_empty(self):
        assert _parse_proto(b"") == {}

    def test_varint_field(self):
        data = _encode_field_varint(6, 999)
        fields = _parse_proto(data)
        assert fields[6] == 999

    def test_string_field(self):
        data = _encode_field_string(3, "PENGUUSDT")
        fields = _parse_proto(data)
        assert fields[3] == "PENGUUSDT"

    def test_bytes_field_non_utf8(self):
        # Invalid UTF-8, kept as raw bytes
        raw = bytes([0xFF, 0xFE, 0x80])
        data = _encode_field_bytes(1, raw)
        fields = _parse_proto(data)
        assert fields[1] == raw

    def test_multiple_fields(self):
        data = (
            _encode_field_string(1, "channel")
            + _encode_field_varint(6, 12345)
            + _encode_field_string(3, "SYM")
        )
        fields = _parse_proto(data)
        assert fields[1] == "channel"
        assert fields[6] == 12345
        assert fields[3] == "SYM"

    def test_unknown_wire_type_stops(self):
        # Wire type 5 (32-bit fixed) is not handled, parser stops
        tag_with_wire5 = (10 << 3) | 5  # field 10, wire type 5
        data = (
            _encode_field_varint(1, 42)
            + _encode_varint(tag_with_wire5)
            + bytes([0, 0, 0, 0])  # 4 bytes of junk
            + _encode_field_varint(2, 99)  # this should NOT be parsed
        )
        fields = _parse_proto(data)
        assert fields[1] == 42
        assert 2 not in fields


# ---------------------------------------------------------------------------
# _decode
# ---------------------------------------------------------------------------

class TestDecode:
    def test_valid_frame(self):
        frame = _build_frame(
            symbol="PENGUUSDT",
            timestamp=1700000000000,
            inner=_build_inner("0.123", "100.0", "0.124", "200.0"),
        )
        result = _decode(frame)
        assert result is not None
        assert result["exchange"] == "mexc"
        assert result["ticker"] == "PENGU"
        assert result["timestamp"] == 1700000000000
        assert result["bid_price"] == pytest.approx(0.123)
        assert result["bid_quantity"] == pytest.approx(100.0)
        assert result["ask_price"] == pytest.approx(0.124)
        assert result["ask_quantity"] == pytest.approx(200.0)

    def test_strips_usdt_suffix(self):
        frame = _build_frame(symbol="BTCUSDT")
        result = _decode(frame)
        assert result["ticker"] == "BTC"

    def test_missing_field_315_returns_none(self):
        # Frame without the nested bookTicker field
        frame = (
            _encode_field_string(1, "channel")
            + _encode_field_string(3, "PENGUUSDT")
            + _encode_field_varint(6, 1700000000000)
        )
        assert _decode(frame) is None

    def test_missing_symbol_returns_none(self):
        frame = (
            _encode_field_string(1, "channel")
            + _encode_field_varint(6, 1700000000000)
            + _encode_field_bytes(315, _build_inner())
        )
        assert _decode(frame) is None

    def test_missing_timestamp_returns_none(self):
        frame = (
            _encode_field_string(1, "channel")
            + _encode_field_string(3, "PENGUUSDT")
            + _encode_field_bytes(315, _build_inner())
        )
        assert _decode(frame) is None

    def test_incomplete_inner_returns_none(self):
        # Inner proto with only fields 1 and 2 (missing 3 and 4)
        inner = (
            _encode_field_string(1, "0.123")
            + _encode_field_string(2, "100.0")
        )
        frame = _build_frame(inner=inner)
        assert _decode(frame) is None

    def test_inner_as_utf8_string(self):
        # If the nested bytes happen to be valid UTF-8, _parse_proto stores
        # them as str. _decode must re-encode to latin-1 before re-parsing.
        inner_bytes = _build_inner("0.5", "10.0", "0.6", "20.0")
        # Verify the inner bytes are valid UTF-8 (they are, since all values are ASCII)
        assert inner_bytes.decode("utf-8")  # should not raise
        frame = _build_frame(inner=inner_bytes)
        result = _decode(frame)
        assert result is not None
        assert result["bid_price"] == pytest.approx(0.5)
