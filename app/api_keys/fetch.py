from __future__ import annotations
from dataclasses import dataclass
from uuid import UUID

from app.utils import (
    INT32,
    INT64,
    INT8,
    encode_compact_array,
    encode_error_code,
    encode_tag_buffer,
    encode_var_int,
)


# https://kafka.apache.org/protocol.html#The_Messages_Fetch


@dataclass
class FetchRequest_V17:
    max_wait_ms: int
    min_bytes: int
    max_bytes: int
    isolation_level: int
    session_id: int
    session_epoch: int
    topics: list[FetchRequest_V17Topic]
    forgotten_topics_data: list[FetchForgottenTopic]
    rack_id: str
    tag_buffer: int


@dataclass
class FetchRequest_V17Topic:
    topic_id: UUID
    partitions: list[FetchRequest_V17Partition]
    tag_buffer: int


@dataclass
class FetchRequest_V17Partition:
    partition: int
    current_leader_epoch: int
    fetch_offset: int
    last_fetched_epoch: int
    log_start_offset: int
    partition_max_bytes: int
    tag_buffer: int


@dataclass
class FetchForgottenTopic:
    topic_id: UUID
    partitions: list[int]
    tag_buffer: int


@dataclass
class FetchResponse_V17:
    throttle_time_ms: int
    error_code: int
    session_id: int
    responses: list[FetchResponses_v17]
    tag_buffer: int

    def encode(self) -> bytes:
        throtle_time_ms = self.throttle_time_ms.to_bytes(INT32)
        error_code = encode_error_code(self.error_code)
        session_id = self.session_id.to_bytes(INT32)
        responses = encode_compact_array(self.responses, lambda r: r.encode())
        tag = encode_tag_buffer(self.tag_buffer)
        return throtle_time_ms + error_code + session_id + responses + tag


@dataclass
class FetchResponses_v17:
    topic_id: UUID
    partitions: list[FetchPartitonResponse]
    tag_buffer: int

    def encode(self) -> bytes:
        partitions = encode_compact_array(self.partitions, lambda p: p.encode())
        tag = encode_tag_buffer(self.tag_buffer)
        return self.topic_id.bytes + partitions + tag


@dataclass
class FetchPartitonResponse:
    partition_index: int
    error_code: int
    high_watermark: int
    last_stable_offset: int
    log_start_offset: int
    aborted_transactions: list[FetchAbortedTransaction]
    preferred_read_replica: int
    records: bytes  # COMPACT_RECORDS
    tag_buffer: int

    def encode(self) -> bytes:
        index = self.partition_index.to_bytes(INT32)
        error_code = encode_error_code(self.error_code)
        high_watermark = self.high_watermark.to_bytes(INT64)
        last_stable_offset = self.last_stable_offset.to_bytes(INT64)
        log_start_offset = self.log_start_offset.to_bytes(INT64)
        aborted_transactions = encode_compact_array(
            self.aborted_transactions, lambda a: a.encode()
        )
        preferred_read_replica = self.preferred_read_replica.to_bytes(INT32)
        size_records = len(self.records)
        size_records = encode_var_int(size_records)
        records_bytes = (1).to_bytes(INT8) if size_records == 0 else self.records

        records = size_records + records_bytes


        tag = encode_tag_buffer(self.tag_buffer)

        return (
            index
            + error_code
            + high_watermark
            + last_stable_offset
            + log_start_offset
            + aborted_transactions
            + preferred_read_replica
            + records
            + tag
        )


@dataclass
class FetchAbortedTransaction:
    producer_id: int
    first_offset: int
    tag_buffer: int

    def encode(self) -> bytes:
        id = self.producer_id.to_bytes(INT64)
        offset = self.first_offset.to_bytes(INT64)
        tag = encode_tag_buffer(self.tag_buffer)
        return id + offset + tag
