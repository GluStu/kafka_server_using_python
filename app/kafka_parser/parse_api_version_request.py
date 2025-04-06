from app.api_keys.api_version import ApiVersionsRequest
from .parser_utils import parse_compact_string, digest
from app.utils import INT8


def parse_api_version_request(data: bytes) -> ApiVersionsRequest:
    name, data = parse_compact_string(data)
    version, data = parse_compact_string(data)
    tag_buffer = digest(data, INT8)

    return ApiVersionsRequest(
        client_software_name=name,
        client_software_version=version,
    )
