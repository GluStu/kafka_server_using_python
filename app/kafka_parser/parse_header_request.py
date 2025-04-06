from app.header_request import HeaderRequest
from .parser_utils import parse_int, parse_nullable_string
from app.utils import INT32, INT16, INT8


def parse_header_request(data: bytes) -> tuple[HeaderRequest, bytes]:
    msg_size, data = parse_int(data, INT32)
    api_key, data = parse_int(data, INT16)
    api_version, data = parse_int(data, INT16)
    correlation_id, data = parse_int(data, INT32)
    is_header_v0 = len(data) == 0

    if is_header_v0:
        client_id = ""
    else:
        client_id, data = parse_nullable_string(data)

    is_header_v2 = len(data) != 0

    if is_header_v2:
        tag_buffer, data = parse_int(data, INT8)
    else:
        tag_buffer = None

    return HeaderRequest(
        msg_size,
        api_key,
        api_version,
        correlation_id,
        client_id,
        tag_buffer,
    ), data
