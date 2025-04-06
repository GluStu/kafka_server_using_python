from uuid import UUID
from dataclasses import dataclass
from typing import Optional

from .api_key import ErrorCode


from ..utils import (
    INT16,
    INT32,
    INT8,
    NULL,
    encode_compact_array,
    encode_compact_string,
    encode_compact_nullable_string,
    encode_int,
)


@dataclass
class DescribeTopicCursor:
    topic_name: str
    partition_index: int


@dataclass
class DescribeTopicPartitionsRequest:
    topics: list[str]
    reponse_partition_limit: int
    cursor: Optional[DescribeTopicCursor]


@dataclass
class DescribePartitionResponse:
    error_code: ErrorCode
    partition_index: int
    leader_id: int
    leader_epoch: int
    replica_nodes: list[int]
    isr_nodes: list[int]
    eligible_leader_replicas: list[int]
    last_known_elr: list[int]
    offline_replicas: list[int]
    tag_buffer: int

    def encode(self):
        error_code = self.error_code.value.to_bytes(INT16)
        p_index = self.partition_index.to_bytes(INT32)
        leader_id = self.leader_id.to_bytes(INT32)
        leader_epoch = self.leader_epoch.to_bytes(INT32)

        encode_int_32 = encode_int(INT32)
        replicas_nodes = encode_compact_array(self.replica_nodes, encode_int_32)
        isr_nodes = encode_compact_array(self.isr_nodes, encode_int_32)
        eligible_leader_replicas = encode_compact_array(
            self.eligible_leader_replicas, encode_int_32
        )
        last_known_elr = encode_compact_array(self.last_known_elr, encode_int_32)
        offile_replicas = encode_compact_array(self.offline_replicas, encode_int_32)
        tag_buffer = self.tag_buffer.to_bytes(INT8)

        return (
            error_code
            + p_index
            + leader_id
            + leader_epoch
            + replicas_nodes
            + isr_nodes
            + eligible_leader_replicas
            + last_known_elr
            + offile_replicas
            + tag_buffer
        )


@dataclass
class DescribeTopicResponse:
    error_code: ErrorCode
    topic: str
    topic_id: UUID
    is_internal: bool
    partitions: list[DescribePartitionResponse]
    topic_authorized_operations: int
    tag_buffer: int

    def encode(self) -> bytes:
        error_code = self.error_code.value.to_bytes(INT16)
        name = encode_compact_nullable_string(self.topic)
        is_internal = self.is_internal.to_bytes(INT8)

        tag_buffer = self.tag_buffer.to_bytes(INT8)

        partitions = encode_compact_array(self.partitions, lambda p: p.encode())

        topic_authorized_operations = self.topic_authorized_operations.to_bytes(INT32)

        return (
            error_code
            + name
            + self.topic_id.bytes
            + is_internal
            + partitions
            + topic_authorized_operations
            + tag_buffer
        )


@dataclass
class DescribeTopicPartitionResponse:
    version: int
    tag_buffer: int
    throttle_time_ms: int
    topics: list[DescribeTopicResponse]
    next_cursor: Optional[DescribeTopicCursor]

    def encode(self) -> bytes:
        tag_buffer = self.tag_buffer.to_bytes(INT8)
        throttle_time_ms = self.throttle_time_ms.to_bytes(INT32)

        topics = encode_compact_array(self.topics, lambda t: t.encode())
        if self.next_cursor is None:
            next_cursor = NULL.to_bytes(INT8)
        else:
            next_cursor = cursor_to_bytes(self.next_cursor)
        match self.version:
            case 0:
                return throttle_time_ms + topics + next_cursor + tag_buffer
            case _:
                return ErrorCode.UNSUPPORTED_VERSION.value.to_bytes(INT16)


def cursor_to_bytes(cursor: DescribeTopicCursor) -> bytes:
    topic_name = encode_compact_string(cursor.topic_name)
    partition_index = cursor.partition_index.to_bytes(INT32)
    return topic_name + partition_index
