from __future__ import annotations

from dataclasses import dataclass
from typing import Optional
from uuid import UUID
from app.kafka_parser.parser_utils import (
    digest,
    parse_compact_string,
    parse_int,
    assert_remain_bytes_is_zero,
    parse_compact_array,
    parse_uuid,
    zigzag_decode,
    parse_varint,
)
from app.utils import (
    INT16,
    INT32,
    INT64,
    INT8,
)


@dataclass
class PartitionRecordValue:
    id: int
    topic_uuid: UUID
    replicas: list[int]
    sync_replicas: list[int]
    removing_replicas: list[int]
    adding_replicas: list[int]
    leader: int
    leader_epoch: int
    partition_epoch: int
    directories: list[UUID]


@dataclass
class FeatureLevelRecordValue:
    name: str
    feature_level: int


@dataclass
class TopicRecord:
    name: str
    uuid: UUID


RecordValue = PartitionRecordValue | FeatureLevelRecordValue | TopicRecord


@dataclass
class Record:
    timestamp_delta: int
    value: RecordValue


@dataclass
class BatchRecords:
    id: int
    records: list[Record]
    base_timestamp: int
    max_timestamp: int
    producer: Optional[Producer]


@dataclass
class Producer:
    id: int
    epoch: int


def parse_kafka_cluster_log(data: bytes) -> dict[int, BatchRecords]:
    batchs = {}
    while len(data) != 0:
        batch_id, data = parse_int(data, INT64)
        batch_length, data = parse_int(data, INT32)

        batch_bytes, data = digest(data, batch_length)

        assert len(batch_bytes) == batch_length
        batch = parse_batch_record(batch_bytes, batch_id)
        batchs[batch.id] = batch
    return batchs


def parse_batch_record(data: bytes, id: int):
    partition_leader_epoch, data = digest(data, INT32)
    magic_byte, data = digest(data, INT8)
    crc, data = digest(data, INT32)
    attributes, data = digest(data, INT16)
    last_offset_delta, data = digest(data, INT32)
    base_timestamp, data = parse_int(data, INT64)
    max_timestamp, data = parse_int(data, INT64)

    producer, data = parser_batch_producer(data)
    base_sequence, data = digest(data, INT32)
    n_records, data = parse_int(data, INT32)

    records = []

    for _ in range(n_records):
        record_length, data = parse_varint(data)
        record_length = zigzag_decode(record_length)
        record_bytes, data = digest(data, record_length)

        assert (
            len(record_bytes) == record_length
        ), f"record length {record_length} {len(record_bytes)}"
        record = parse_record(record_bytes)
        records.append(record)

    return BatchRecords(
        id,
        records,
        base_timestamp,
        max_timestamp,
        producer,
    )


def parser_batch_producer(data: bytes) -> tuple[Optional[Producer], bytes]:
    producer_id, data = parse_int(data, INT64)
    producer_epoch, data = parse_int(data, INT16)

    not_set_id = b"\xff" * INT64
    not_set_epoch = b"\xff" * INT16
    not_set = not_set_id + not_set_epoch

    if producer_id + producer_epoch == not_set:
        producer = None
    else:
        producer = Producer(producer_id, producer_epoch)

    return producer, data


def parse_record(data: bytes) -> Record:
    attributes, data = digest(data, INT8)
    timestamp_delta, data = parse_int(data, INT8)
    offset_delta, data = digest(data, INT8)
    record_key, data = parse_record_key(data)

    length, data = parse_varint(data)
    length = zigzag_decode(length)
    value_bytes, data = digest(data, length)

    # print(", ".join([hex(b) for b in data]))
    record_value = parse_record_value(value_bytes)
    headers_array_count, data = digest(data, INT8)
    assert_remain_bytes_is_zero(data)

    return Record(timestamp_delta, record_value)


def parse_record_value(value_bytes: bytes) -> RecordValue:
    frame_version, value_bytes = parse_int(value_bytes, INT8)
    type_record_value, value_bytes = parse_int(value_bytes, INT8)
    version_record_value, value_bytes = parse_int(value_bytes, INT8)
    # rec = RecordValue(frame_version, version_record_value)

    match type_record_value:
        case 12:
            return parse_feature_level_record_value(value_bytes)
        case 2:
            return parse_topic_record_value(value_bytes)
        case 3:
            return parse_partition_record_value(value_bytes)
        case _:
            raise NotImplementedError(
                f"type record {type_record_value}, {frame_version,version_record_value}"
            )


def parse_partition_record_value(data: bytes) -> PartitionRecordValue:
    id, data = parse_int(data, INT32)
    toppic_uuid, data = parse_uuid(data)

    replicas, data = parse_compact_arrayi32(data)
    sync_replicas, data = parse_compact_arrayi32(data)
    removing_replicas, data = parse_compact_arrayi32(data)
    adding_replicas, data = parse_compact_arrayi32(data)
    leader, data = parse_int(data, INT32)
    leader_epoch, data = parse_int(data, INT32)
    partition_epoch, data = parse_int(data, INT32)
    directories, data = parse_compact_array(data, parse_uuid)
    tagged_fields_count, data = digest(data, INT8)
    assert tagged_fields_count == b"\x00"

    return PartitionRecordValue(
        id,
        toppic_uuid,
        replicas,
        sync_replicas,
        removing_replicas,
        adding_replicas,
        leader,
        leader_epoch,
        partition_epoch,
        directories,
    )


def parse_compact_arrayi32(data: bytes) -> tuple[list[int], bytes]:
    return parse_compact_array(data, lambda d: parse_int(d, INT32))


def parse_compact_arrayuuid(data: bytes) -> tuple[list[UUID], bytes]:
    return parse_compact_array(data, parse_uuid)


def parse_feature_level_record_value(data: bytes) -> FeatureLevelRecordValue:
    name, data = parse_compact_string(data)
    feature_level, data = parse_int(data, INT16)
    taggeed_field_count, data = parse_int(data, INT8)

    assert taggeed_field_count == 0

    assert_remain_bytes_is_zero(data)
    return FeatureLevelRecordValue(
        name,
        feature_level,
    )


def parse_topic_record_value(data: bytes) -> TopicRecord:
    name, data = parse_compact_string(data)
    uuid, data = parse_uuid(data)
    taggeed_field_count, data = digest(data, INT8)

    assert_remain_bytes_is_zero(data)

    return TopicRecord(name, uuid)


def parse_record_key(data: bytes) -> tuple[Optional[str], bytes]:
    null = -1

    key_length, data = parse_int(data, INT8)
    length = zigzag_decode(key_length)

    if length == null:
        return None, data

    raise NotImplementedError(f"key length {key_length}")
