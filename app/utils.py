from typing import Callable


INT8 = 1
INT16 = 2
INT32 = 4
INT64 = 8
NULL = int.from_bytes(b"\xff")


# split(n)
# b1,b2 = b[:n],b[:n]
# digest(type):
# b1,b2 = b[:type.n],b[:type.n]
# return type.from_bytes(b1), b2


def str_from_bytes(b: bytes) -> str:
    return b.decode()


def sum_bytes(data: list[bytes]) -> bytes:
    acc = data[0]
    for b in data[1:]:
        acc = acc + b

    return acc


def encode_compact_nullable_string(string: str) -> bytes:
    if len(string) == 0:
        return NULL.to_bytes(INT8)
    else:
        return encode_compact_string(string)


def encode_int(type_int: int) -> Callable[[int], bytes]:
    return lambda x: x.to_bytes(type_int)


def encode_error_code(code: int) -> bytes:
    return code.to_bytes(INT16)


def encode_tag_buffer(tag: int):
    return tag.to_bytes(INT8)


def encode_compact_array[T](array: list[T], encode: Callable[[T], bytes]) -> bytes:
    size_array = len(array)
    if size_array == 0:
        return (1).to_bytes(INT8)
    array_bytes = [encode(v) for v in array]

    return (size_array + 1).to_bytes(INT8) + sum_bytes(array_bytes)


def encode_compact_string(string: str) -> bytes:
    size_str = len(string) + 1
    encode_str = string.encode()
    return size_str.to_bytes(INT8) + encode_str


def encode_var_int(v: int) -> bytes:
    if v <= 0b1111111:
        return v.to_bytes(INT8)

    bits = bin(v)[2:]

    first = bits[-7:]
    second = bits[:-7]

    return int(f"1{first}", 2).to_bytes(INT8) + encode_var_int(int(second, 2))
