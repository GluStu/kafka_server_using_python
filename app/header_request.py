from dataclasses import dataclass
from typing import Optional

from app.api_keys.api_key import ApiKeys, ErrorCode
from app.utils import INT16, INT32, INT8


@dataclass
class HeaderRequest:
    msg_size: int
    api_key: int
    api_version: int
    correlation_id: int
    client_id: str
    tag_buffer: Optional[int]

    def encode(self) -> bytes:
        id = self.correlation_id.to_bytes(INT32)

        if self.api_key == ApiKeys.ApiVersions.value.code:
            return id

        if self.tag_buffer is None:
            return id
        else:
            tag_buffer = self.tag_buffer.to_bytes(INT8)

            return id + tag_buffer


@dataclass
class UnknownApiKeyResponse:
    api_key: int

    def encode(self) -> bytes:
        error = ErrorCode.UNKNOWN.value.to_bytes(INT16)
        key = self.api_key.to_bytes(INT32)

        return key + error
