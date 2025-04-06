from app.util import (
    read_signed_varint_from_file,
    read_unsigned_varint_from_file,
    int_to_var_int,
)
from collections import defaultdict
class Partition:
    def __init__(
        self,
        partition_index,
        leader_id,
        leader_epoch,
        replica_nodes,
        isr_nodes,
        eligible_leader_replicas,
        last_known_elr,
        offline_replicas,
    ):
        self.partition_index = partition_index
        self.leader_id = leader_id
        self.leader_epoch = leader_epoch
        self.replica_nodes = replica_nodes
        self.isr_nodes = isr_nodes
    def get_partition_byte_representation(self):
        repr = bytearray()
        # error code
        repr.extend((0).to_bytes(2, "big"))
        repr.extend(self.partition_index.to_bytes(4, "big"))
        repr.extend(self.leader_id.to_bytes(4, "big"))
        repr.extend(self.leader_epoch.to_bytes(4, "big"))
        repr.extend(int_to_var_int(len(self.replica_nodes) + 1))
        for replica_node in self.replica_nodes:
            repr.extend(replica_node.to_bytes(4, "big"))
        repr.extend(int_to_var_int(len(self.isr_nodes) + 1))
        for isr_node in self.isr_nodes:
            repr.extend(isr_node.to_bytes(4, "big"))
        # no eligible leaders
        repr.extend(int_to_var_int(1))
        # no last known elrs
        repr.extend(int_to_var_int(1))
        # no offline replicas
        repr.extend(int_to_var_int(1))
        # no tag buffers
        repr.extend(int_to_var_int(0))
        return repr
class MetadataFile:
    def __init__(self, filename):
        f = open(filename, "rb")
        self.topic_map = {}
        self.topic_uuid_to_partition_map = defaultdict(list)
        for _ in range(30):
            self.base_offset = int.from_bytes(f.read(8), "big")
            self.batch_length = int.from_bytes(f.read(4), "big")
            # print(hex(self.batch_length))
            self.partition_leader_epoch = int.from_bytes(f.read(4), "big")
            self.magic_byte = int.from_bytes(f.read(1), "big")
            self.check_sum = int.from_bytes(f.read(4), "big")
            self.attributes = int.from_bytes(f.read(2), "big")
            self.last_offset_delta = int.from_bytes(f.read(4), "big")
            self.base_timestamp = int.from_bytes(f.read(8), "big")
            self.max_timestamp = int.from_bytes(f.read(8), "big")
            self.producer_id = int.from_bytes(f.read(8), "big")
            self.producer_epoch = int.from_bytes(f.read(2), "big")
            self.base_sequence = int.from_bytes(f.read(4), "big")
            self.records_length = int.from_bytes(f.read(4), "big")
            for _ in range(self.records_length):
                record_length = read_signed_varint_from_file(f)
                attributes = int.from_bytes(f.read(1), "big")
                timestamp_delta = int.from_bytes(f.read(1), "big")
                offset_delta = read_signed_varint_from_file(f)
                key_length = read_signed_varint_from_file(f)
                value_length = read_signed_varint_from_file(f)
                value_frame_version = int.from_bytes(f.read(1), "big")
                value_type = int.from_bytes(f.read(1), "big")
                if value_type == 12:
                    value_feature_version = int.from_bytes(f.read(1), "big")
                    name_length = read_unsigned_varint_from_file(f)
                    name = f.read(name_length - 1).decode("utf-8")
                    feature_level = int.from_bytes(f.read(2), "big")
                    tagged_fields = read_unsigned_varint_from_file(f)
                elif value_type == 2:
                    # topic
                    value_feature_version = int.from_bytes(f.read(1), "big")
                    name_length = read_unsigned_varint_from_file(f)
                    name = f.read(name_length - 1).decode("utf-8")
                    topic_uuid = int.from_bytes(f.read(16), "big")
                    tagged_fields = read_unsigned_varint_from_file(f)
                    self.topic_map[name] = topic_uuid
                elif value_type == 3:
                    # partition
                    value_feature_version = int.from_bytes(f.read(1), "big")
                    partition_id = int.from_bytes(f.read(4), "big")
                    topic_uuid = int.from_bytes(f.read(16), "big")
                    replica_array_size = read_unsigned_varint_from_file(f)
                    replica_nodes = []
                    for _ in range(replica_array_size - 1):
                        replica_id = int.from_bytes(f.read(4), "big")
                        replica_nodes.append(replica_id)
                    in_sync_replica_array_size = read_unsigned_varint_from_file(f)
                    isr_nodes = []
                    for _ in range(in_sync_replica_array_size - 1):
                        in_sync_replica_id = int.from_bytes(f.read(4), "big")
                        isr_nodes.append(in_sync_replica_id)
                    removing_replica_array_size = read_unsigned_varint_from_file(f)
                    adding_replica_array_size = read_unsigned_varint_from_file(f)
                    leader_replica_id = int.from_bytes(f.read(4), "big")
                    leader_epoch = int.from_bytes(f.read(4), "big")
                    partition_epoch = int.from_bytes(f.read(4), "big")
                    directories_array_len = read_unsigned_varint_from_file(f)
                    for _ in range(directories_array_len - 1):
                        directories_uuid = int.from_bytes(f.read(16), "big")
                    tagged_fields = read_unsigned_varint_from_file(f)
                    partition = Partition(
                        partition_id,
                        leader_replica_id,
                        leader_epoch,
                        replica_nodes,
                        isr_nodes,
                        None,
                        None,
                        None,
                    )
                    self.topic_uuid_to_partition_map[topic_uuid].append(partition)
                headers_array_count = read_unsigned_varint_from_file(f)
    def get_partitions_for_topic(self, topic):
        if topic not in self.topic_map:
            return None
        return self.topic_uuid_to_partition_map[self.topic_map[topic]]