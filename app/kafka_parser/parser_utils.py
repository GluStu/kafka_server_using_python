from typing import Callable
from uuid import UUID
from app.utils import INT64, INT8, INT16

COMPACT_STRING_LEN = INT8
NULLABLE_STRING_LEN = INT16


def parse_int(data: bytes, type_int: int) -> tuple[int, bytes]:
    value, data = digest(data, type_int)
    return int.from_bytes(value), data


def parse_tag_buffer(data: bytes) -> tuple[int, bytes]:
    return parse_int(data, INT8)


def digest(data: bytes, size: int) -> tuple[bytes, bytes]:
    return data[:size], data[size:]


def assert_remain_bytes_is_zero(data: bytes):
    assert len(data) == 0, f"remain bytes: {data}\n"


def parse_nullable_string(data: bytes) -> tuple[str, bytes]:
    size_str, data = parse_int(data, NULLABLE_STRING_LEN)

    string_bytes, data = digest(data, size_str)
    return string_bytes.decode(), data


def parse_compact_string(data: bytes) -> tuple[str, bytes]:
    size_str, data = parse_int(data, COMPACT_STRING_LEN)

    string_bytes, data = digest(data, size_str - 1)
    return string_bytes.decode(), data


def parse_uuid(data: bytes) -> tuple[UUID, bytes]:
    uuid, data = digest(data, INT64 * 2)

    return UUID(bytes=uuid), data


def parse_compact_array[T](
    data: bytes, callback: Callable[[bytes], tuple[T, bytes]]
) -> tuple[list[T], bytes]:
    length, data = parse_int(data, INT8)

    array = []
    for _ in range(length - 1):
        value, data = callback(data)
        array.append(value)

    return array, data


def zigzag_decode(n: int) -> int:
    return (n >> 1) ^ (-(n & 1))


"""
https://protobuf.dev/programming-guides/encoding/
"""


def parse_varint(data: bytes) -> tuple[int, bytes]:
    def parse_byte(data: bytes) -> tuple[str, bytes]:
        var_int_bytes, data = parse_int(data, INT8)
        string_bits = bin(var_int_bytes)
        string_bits = string_bits[2:].zfill(8)
        return string_bits, data

    first_string, data = parse_byte(data)
    result_string = first_string[1:]
    while first_string[0] == "1":
        first_string, data = parse_byte(data)
        result_string = first_string[1:] + result_string

    return int(result_string, 2), data
