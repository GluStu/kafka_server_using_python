from dataclasses import dataclass
from ..utils import INT16, INT32, INT8, encode_compact_array, sum_bytes
from .api_key import ApiKey, ErrorCode


@dataclass
class ApiVersionsRequest:
    client_software_name: str
    client_software_version: str


@dataclass
class ApiKeysResponse(ApiKey):
    tag_buffer: int


@dataclass
class ApiVersionsResponse:
    version: int
    error_code: int
    api_keys: list[ApiKeysResponse]
    throttle_time_ms: int
    tag_buffer: int

    def encode(self) -> bytes:
        throttle_time_ms = (0).to_bytes(INT32)

        if self.version <= 2:

            def key_to_bytes(key: ApiKeysResponse):
                return self.api_key_to_bytes(key)
        else:

            def key_to_bytes(key: ApiKeysResponse):
                return self.api_key_to_bytes(key) + key.tag_buffer.to_bytes(INT8)

        api_keys = encode_compact_array(self.api_keys, key_to_bytes)
        error_code = self.error_code.to_bytes(INT16)
        match self.version:
            case 0:
                return error_code + api_keys

            case 1 | 2:
                return error_code + api_keys + throttle_time_ms
            case 3 | 4:
                return (
                    error_code
                    + api_keys
                    + throttle_time_ms
                    + self.tag_buffer.to_bytes(INT8)
                )
            case _:
                return ErrorCode.UNSUPPORTED_VERSION.value.to_bytes(INT16)

    @staticmethod
    def api_key_to_bytes(api_key: ApiKey) -> bytes:
        min_version = api_key.min_version
        max_version = api_key.max_version
        return (
            api_key.code.to_bytes(INT16)
            + min_version.to_bytes(INT16)
            + max_version.to_bytes(INT16)
        )
