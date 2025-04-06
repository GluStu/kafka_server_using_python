from enum import Enum
from dataclasses import dataclass


@dataclass
class ApiKey:
    code: int
    max_version: int
    min_version: int


class ApiKeys(Enum):
    ApiVersions = ApiKey(code=18, min_version=0, max_version=4)
    DescribeTopicPartitions = ApiKey(code=75, min_version=0, max_version=0)
    Fetch = ApiKey(code=1, min_version=0, max_version=17)


class ErrorCode(Enum):
    UNKNOWN = -1
    UNKNOWN_TOPIC_OR_PARTITION = 3
    NONE = 0
    OFFSET_OUT_OF_RANGE = 1
    CORRUPT_MESSAGE = 2
    UNSUPPORTED_VERSION = 35
    UNKNOWN_TOPIC_ID = 100
