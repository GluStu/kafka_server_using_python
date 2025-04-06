from .parser_utils import parse_compact_array, parse_compact_string, digest, parse_int
from app.utils import (
    INT32,
    INT8,
    NULL,
)
from app.api_keys import (
    DescribeTopicCursor,
    DescribeTopicPartitionsRequest,
)


def parse_describe_topic_partition_request(
    body_bytes: bytes,
) -> DescribeTopicPartitionsRequest:
    def compact_string_with_tag(body_bytes: bytes) -> tuple[str, bytes]:
        name, body_bytes = parse_compact_string(body_bytes)
        tag_buffer, body_bytes = digest(body_bytes, INT8)
        return name, body_bytes

    topics, body_bytes = parse_compact_array(body_bytes, compact_string_with_tag)

    reponse_partition_limit, body_bytes = parse_int(body_bytes, INT32)

    if body_bytes[0] == NULL:
        cursor = None
    else:
        topic_name, body_bytes = parse_compact_string(body_bytes)
        partition_index, body_bytes = parse_int(body_bytes, INT32)
        tag_buffer, body_bytes = digest(body_bytes, INT8)
        cursor = DescribeTopicCursor(
            topic_name=topic_name,
            partition_index=partition_index,
        )

    return DescribeTopicPartitionsRequest(
        topics=topics,
        reponse_partition_limit=reponse_partition_limit,
        cursor=cursor,
    )
