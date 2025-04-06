from typing import Optional
from uuid import UUID, uuid4
from app.api_keys.fetch import (
    FetchForgottenTopic,
    FetchRequest_V17,
    FetchRequest_V17Partition,
    FetchRequest_V17Topic,
)
from app.utils import INT32, INT64, INT8
from .parser_utils import (
    assert_remain_bytes_is_zero,
    parse_compact_string,
    parse_int,
    parse_compact_array,
    parse_tag_buffer,
    parse_uuid,
)


def parse_fetch_request(data: bytes) -> FetchRequest_V17:
    max_wait_ms, data = parse_int(data, INT32)
    min_bytes, data = parse_int(data, INT32)
    max_bytes, data = parse_int(data, INT32)
    isolation_level, data = parse_int(data, INT8)
    session_id, data = parse_int(data, INT32)
    session_epoch, data = parse_int(data, INT32)

    topics, data = parse_compact_array(data, parse_fetch_topics)
    forgotten_topics, data = parse_compact_array(data, parse_fetch_forgotten_topics)

    rack_id, data = parse_compact_string(data)

    tag_buffer, data = parse_tag_buffer(data)

    assert_remain_bytes_is_zero(data)

    return FetchRequest_V17(
        max_wait_ms,
        min_bytes,
        max_bytes,
        isolation_level,
        session_id,
        session_epoch,
        topics,
        forgotten_topics,
        rack_id,
        tag_buffer,
    )


def parse_partition(data: bytes) -> tuple[FetchRequest_V17Partition, bytes]:
    partition, data = parse_int(data, INT32)
    current_leader_epoch, data = parse_int(data, INT32)
    fetch_offset, data = parse_int(data, INT64)
    last_fetched_epoch, data = parse_int(data, INT32)
    last_start_offset, data = parse_int(data, INT64)
    partition_max_bytes, data = parse_int(data, INT32)
    tag, data = parse_tag_buffer(data)
    return FetchRequest_V17Partition(
        partition,
        current_leader_epoch,
        fetch_offset,
        last_fetched_epoch,
        last_start_offset,
        partition_max_bytes,
        tag,
    ), data


def parse_fetch_topics(data: bytes) -> tuple[FetchRequest_V17Topic, bytes]:
    id, data = parse_uuid(data)
    partitions, data = parse_compact_array(data, parse_partition)
    tag, data = parse_tag_buffer(data)

    return FetchRequest_V17Topic(
        topic_id=id, partitions=partitions, tag_buffer=tag
    ), data


def parse_fetch_forgotten_topics(
    data: bytes,
) -> tuple[FetchForgottenTopic, bytes]:
    forgotten_topic_id, data = parse_uuid(data)
    partitions, data = parse_compact_array(data, lambda d: parse_int(d, INT32))
    tag, data = parse_tag_buffer(data)
    return FetchForgottenTopic(forgotten_topic_id, partitions, tag), data
