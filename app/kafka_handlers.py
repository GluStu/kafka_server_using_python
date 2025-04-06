from dataclasses import dataclass
import os
from pathlib import Path
from uuid import UUID

from app.api_keys.api_version import (
    ApiKeysResponse,
    ApiVersionsRequest,
    ApiVersionsResponse,
)
from app.api_keys.describe_topic_partitions import (
    DescribePartitionResponse,
    DescribeTopicPartitionResponse,
    DescribeTopicPartitionsRequest,
    DescribeTopicResponse,
)
from app.api_keys.fetch import (
    FetchAbortedTransaction,
    FetchPartitonResponse,
    FetchRequest_V17,
    FetchRequest_V17Partition,
    FetchRequest_V17Topic,
    FetchResponse_V17,
    FetchResponses_v17,
)
from app.header_request import HeaderRequest
from app.kafka_parser import PartitionRecordValue

from . import api_keys
from . import kafka_parser

from app.api_keys import ErrorCode, ApiKeys
from app.api_keys.api_key import ApiKey

from .utils import INT32


@dataclass
class Context:
    topics: dict[str, UUID]
    topics_partitions: dict[str, list[kafka_parser.PartitionRecordValue]]


def get_log_dir() -> Path:
    log_dir = os.getenv("KAFKA_LOG_DIR")
    if log_dir:
        return Path(log_dir)
    else:
        return Path("/") / Path("tmp") / Path("kraft-combined-logs")


KAFKA_LOG_DIR = get_log_dir()


def load_metada():
    env_path = os.getenv("KAFKA_LOG")
    if env_path is None:
        path = "__cluster_metadata-0/00000000000000000000.log"
    else:
        path = env_path
    kafka_logs = KAFKA_LOG_DIR / path

    global KAFKA_METADA_BYTES
    kafka_log_bytes = kafka_logs.read_bytes()
    KAFKA_METADA_BYTES = kafka_log_bytes
    batchs = kafka_parser.parse_kafka_cluster_log(kafka_log_bytes)

    topics: dict[str, UUID] = {}

    topics_partitions: dict[str, list[kafka_parser.PartitionRecordValue]] = {}
    feature_levels = {}

    for batch in batchs.values():
        for record in batch.records:
            match record.value:
                case kafka_parser.PartitionRecordValue() as value:
                    key = str(value.topic_uuid)
                    if topics_partitions.get(key) is None:
                        topics_partitions[key] = []
                    topics_partitions[key].append(value)

                case kafka_parser.TopicRecord() as value:
                    topics[value.name] = value.uuid
                case kafka_parser.FeatureLevelRecordValue() as value:
                    feature_levels[value.name] = value.feature_level

    return Context(topics, topics_partitions)


CONTEXT = load_metada()


def get_context() -> Context:
    return CONTEXT


def describe_topic_partitions(
    header: HeaderRequest,
    body: DescribeTopicPartitionsRequest,
) -> DescribeTopicPartitionResponse:
    context = get_context()

    def to_topic_response(topic_name: str) -> DescribeTopicResponse:
        topic_uuid = context.topics.get(topic_name)

        if topic_uuid is None:
            return DescribeTopicResponse(
                error_code=ErrorCode.UNKNOWN_TOPIC_OR_PARTITION,
                topic=topic_name,
                topic_id=UUID(bytes=(0).to_bytes(INT32 * 4)),
                is_internal=False,
                partitions=[],
                topic_authorized_operations=0x00000DF8,
                tag_buffer=0,
            )

        partitions = context.topics_partitions.get(str(topic_uuid), [])

        def to_partiton_response(
            p: kafka_parser.PartitionRecordValue,
        ) -> DescribePartitionResponse:
            return DescribePartitionResponse(
                error_code=ErrorCode.NONE,
                partition_index=p.id,
                leader_id=p.leader,
                leader_epoch=p.leader_epoch,
                replica_nodes=p.replicas,
                isr_nodes=p.sync_replicas,
                eligible_leader_replicas=[],
                last_known_elr=[],
                offline_replicas=p.removing_replicas,
                tag_buffer=0,
            )

        response_partitions = [to_partiton_response(p) for p in partitions]

        return DescribeTopicResponse(
            error_code=ErrorCode.NONE,
            topic=topic_name,
            topic_id=topic_uuid,
            is_internal=False,
            partitions=response_partitions,
            topic_authorized_operations=0x00000DF8,
            tag_buffer=0,
        )

    topics = [to_topic_response(t) for t in body.topics]

    return DescribeTopicPartitionResponse(
        version=header.api_version,
        tag_buffer=0,
        throttle_time_ms=0,
        topics=topics,
        next_cursor=body.cursor,
    )


def api_versions(
    header: HeaderRequest, body: ApiVersionsRequest
) -> ApiVersionsResponse:
    def api_keys_to_response(k: ApiKey) -> ApiKeysResponse:
        return ApiKeysResponse(
            code=k.code,
            min_version=k.min_version,
            max_version=k.max_version,
            tag_buffer=0,
        )

    keys = [api_keys_to_response(k.value) for k in ApiKeys]

    return ApiVersionsResponse(
        version=header.api_version,
        error_code=ErrorCode.NONE.value,
        api_keys=keys,
        throttle_time_ms=0,
        tag_buffer=0,
    )


def fetch(header: HeaderRequest, body: FetchRequest_V17) -> FetchResponse_V17:
    return FetchResponse_V17(
        error_code=ErrorCode.NONE.value,
        throttle_time_ms=0,
        session_id=0,
        responses=[to_response(t) for t in body.topics],
        tag_buffer=0,
    )


def find_topic_name(topic_id: UUID) -> str | None:
    context = get_context()
    recorded_topic = list(
        filter(lambda item: item[1] == topic_id, context.topics.items())
    )

    len_recorded_topic = len(recorded_topic)
    if len_recorded_topic == 0:
        return None

    assert len_recorded_topic == 1, f"more topics founded {recorded_topic}"
    topic_name, topic_uuid = recorded_topic[0]
    return topic_name


def to_response(topic: FetchRequest_V17Topic) -> FetchResponses_v17:
    topic_name = find_topic_name(topic.topic_id)
    if topic_name is None:
        return FetchResponses_v17(
            topic.topic_id,
            [
                FetchPartitonResponse(
                    partition_index=0,
                    error_code=ErrorCode.UNKNOWN_TOPIC_ID.value,
                    high_watermark=0,
                    last_stable_offset=0,
                    log_start_offset=0,
                    aborted_transactions=[],
                    preferred_read_replica=0,
                    records=bytes(),
                    tag_buffer=0,
                )
            ],
            0,
        )

    return FetchResponses_v17(
        topic.topic_id,
        [to_partition_response(p.partition, topic_name) for p in topic.partitions],
        0,
    )


def to_partition_response(
    partition_index: int, topic_name: str
) -> FetchPartitonResponse:
    records_dir = KAFKA_LOG_DIR / Path(f"{topic_name}-{partition_index}")
    records_file = records_dir / f"{0:020}.log"

    return FetchPartitonResponse(
        partition_index=partition_index,
        error_code=ErrorCode.NONE.value,
        high_watermark=0,
        last_stable_offset=0,
        log_start_offset=0,
        aborted_transactions=[],
        preferred_read_replica=0,
        records=records_file.read_bytes(),
        tag_buffer=0,
    )


def get_handles():
    return {
        api_keys.ApiKeys.ApiVersions.value.code: (
            kafka_parser.parse_api_version_request,
            api_versions,
        ),
        api_keys.ApiKeys.DescribeTopicPartitions.value.code: (
            kafka_parser.parse_describe_topic_partition_request,
            describe_topic_partitions,
        ),
        api_keys.ApiKeys.Fetch.value.code: (kafka_parser.parse_fetch_request, fetch),
    }
