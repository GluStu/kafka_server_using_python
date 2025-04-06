from dataclasses import dataclass

from app.header_request import HeaderRequest
from .kafka_encode import KafkaEncode
from app.utils import INT32


@dataclass
class KafkaResponse:
    header: HeaderRequest
    response_body: KafkaEncode

    def encode(self) -> bytes:
        msg = self.header.encode() + self.response_body.encode()

        msg_size = len(msg)

        return msg_size.to_bytes(INT32) + msg
