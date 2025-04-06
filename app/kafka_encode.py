from typing import Protocol


class KafkaEncode(Protocol):
    def encode(self) -> bytes: ...
