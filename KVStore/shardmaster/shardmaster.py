import logging
import threading
import grpc

from KVStore.tests.utils import KEYS_LOWER_THRESHOLD, KEYS_UPPER_THRESHOLD
from KVStore.protos.kv_store_pb2 import RedistributeRequest, ServerRequest
from KVStore.protos.kv_store_pb2_grpc import KVStoreStub
from KVStore.protos.kv_store_shardmaster_pb2_grpc import ShardMasterServicer
from KVStore.protos.kv_store_shardmaster_pb2 import *
from typing import Dict, Tuple

from google.protobuf import empty_pb2 as google_dot_protobuf_dot_empty__pb2
from multiprocessing import Manager


logger = logging.getLogger(__name__)


def grpc_redistribute(source_server: str, destination_server: str, keys: Tuple[int, int]):
    redistribute_request = RedistributeRequest()
    redistribute_request.destination_server = destination_server
    redistribute_request.lower_val = keys[0]
    redistribute_request.upper_val = keys[1]
    print(f"Source server: {source_server}, Destination server: {destination_server}, Keys: {keys}")
    with grpc.insecure_channel(source_server) as channel:
        stub = KVStoreStub(channel)
        response = stub.Redistribute(redistribute_request)

    print(f"Redistributed keys: {keys} from {source_server} to {destination_server}")


class ShardMasterService:
    def __init__(self):
        self.manager = Manager()
        self.node_dict = self.manager.dict()
        self.servers = self.manager.list()
        self.lock = threading.Lock()

    def join(self, server: str):
        with self.lock:
            if server not in self.servers:
                self.servers.append(server)
                self.recalculate_shards()
                self.redistribute_keys(server)
                print(f"New Join ({server}), Servers -> {self.servers}")
                print(f"Shards -> {self.node_dict}")

    def leave(self, server: str):
        if server in self.servers:
            self.lock.acquire()
            try:
                self.servers.remove(server)
                del self.node_dict[server]
                if len(self.servers) >= 1:
                    self.recalculate_shards()
                    destination = self.servers[0]
                    grpc_redistribute(server, destination, (0, 99))
                    self.redistribute_keys(server)
            finally:
                self.lock.release()
            print(f"New Leave ({server}), Servers -> {self.servers}")
            print(f"Shards -> {self.node_dict}")

    def recalculate_shards(self):
        total_servers = len(self.servers)
        total_keys = 100

        if total_servers == 0:
            return

        avg_keys_per_server = total_keys // total_servers
        remaining_keys = total_keys % total_servers

        lower_val = 0
        for i, server in enumerate(self.servers):
            keys_count = avg_keys_per_server + (1 if i < remaining_keys else 0)
            upper_val = lower_val + keys_count - 1
            self.node_dict[server] = (lower_val, upper_val)
            lower_val = upper_val + 1

    def query(self, key):
        # self.lock.acquire()
        for address in self.servers:
            print(f"Key: {str(key)} Range: {str(self.node_dict.get(address))} Server: {address}")
            if self.node_dict.get(address)[0] <= key <= self.node_dict.get(address)[1]:
                return address
        # self.lock.release()
        return None

    def redistribute_keys(self, server):
        #with self.lock:
        if server in self.servers:
            server_index = self.servers.index(server)
            prev_server = self.servers[server_index - 1] if server_index > 0 else None
            next_server = self.servers[server_index + 1] if server_index < len(self.servers) - 1 else None

            if prev_server:
                shard = self.node_dict[prev_server]
                if shard[1] != 99:
                    destination = server
                    keys = (shard[1] + 1, 99)
                    print(f"Redistributing keys: {keys} from {prev_server} to {destination}")
                    grpc_redistribute(prev_server, destination, keys)

            if next_server:
                shard = self.node_dict[next_server]
                if shard[0] != 0:
                    destination = server
                    keys = (0, shard[0])
                    print(f"Redistributing keys: {keys} from {next_server} to {destination}")
                    grpc_redistribute(next_server, destination, keys)
        else:
            print(f"Server {server} not in servers list")

    def join_replica(self, server: str) -> Role:
        pass

    def query_replica(self, key: int, op: Operation) -> str:
        pass


class ShardMasterSimpleService(ShardMasterService):
    def __init__(self):
        self.server_list = []  # We will save a tuple 3 arguments: (port, lower_bound, upper_bound)
        self.assigned_ranges = dict()  # We will save a tuple 2 arguments: (lower_bound, upper_bound)

    def join(self, server: str):
        self.server_list.append(server)
        #self.rebalance()

    def leave(self, server: str):
        self.server_list.remove(server)
        #self.rebalance()

    def query(self, key: int) -> str:
        for server in self.assigned_ranges:  # server = (port, lower_bound, upper_bound)
            if self.assigned_ranges[server][0] <= key < self.assigned_ranges[server][1]:  # If the key is in the range of the server
                return server  # return port

    # Redistribute all keys to all servers to ensure all the keys are in the right server
    def _rebalance(self):
        # Compute chunk size
        shard_size = (KEYS_UPPER_THRESHOLD - KEYS_LOWER_THRESHOLD) // len(self.server_list)
        shard_remain = (KEYS_UPPER_THRESHOLD - KEYS_LOWER_THRESHOLD) % len(self.server_list)

        # Compute the new distribution of keys before redistributing them
        self.assigned_ranges = dict()
        i=0
        for server in self.server_list:
            if shard_remain > 0:
                current_shard_size  = shard_size + 1
                shard_remain -= 1
            else:
                current_shard_size = shard_size

            self.assigned_ranges[i]=(i * current_shard_size, current_shard_size * (i + 1))   # assigned_ranges = {0: (0, 33), 1: (33, 66), 2: (66, 100)}
            i+=1

        for origin_server in self.server_list: # For each server in the list we will redistribute the keys to the other servers
            for destination_server in self.server_list:
                if origin_server != destination_server: # We don't want to redistribute the keys to the same server
                    channel = grpc.insecure_channel(origin_server)  # Create a channel to the origin server
                    stub = KVStoreStub(channel) # Create a stub to the origin server
                    # Generate gRPC request to RedistributeRequest
                    stub.Redistribute(RedistributeRequest(destination_server=destination_server,
                                                          lower_val=self.assigned_ranges[destination_server][0],
                                                          upper_val=self.assigned_ranges[destination_server][1]))
                    channel.close() # Close the channel to the origin server

class ShardMasterReplicasService(ShardMasterSimpleService):
    def __init__(self, number_of_shards: int):
        super().__init__()
        """
        To fill with your code
        """

    def leave(self, server: str):
        super().leave(server)

    def join_replica(self, server: str) -> Role:
        response = super().join_replica(server)
        if response == "MASTER":
            return Role.MASTER
        elif response == "REPLICA":
            return Role.REPLICA
        else:
            raise ValueError("ROLE NOT FOUND")

    def query_replica(self, key: int, op: Operation) -> str:
        response = super().query_replica(key, op)
        return response


class ShardMasterServicer(ShardMasterServicer):
    def __init__(self, shard_master_service: ShardMasterService):
        self.shard_master_service = shard_master_service

    def Join(self, request: JoinRequest, context) -> google_dot_protobuf_dot_empty__pb2.Empty():
        server = request.server
        self.shard_master_service.join(server)
        return google_dot_protobuf_dot_empty__pb2.Empty()

    def Leave(self, request: LeaveRequest, context) -> google_dot_protobuf_dot_empty__pb2.Empty():
        server = request.server
        self.shard_master_service.leave(server)
        return google_dot_protobuf_dot_empty__pb2.Empty()

    def Query(self, request: QueryRequest, context) -> QueryResponse:
        response = self.shard_master_service.query(request.key)
        toReturn: QueryResponse = QueryResponse(server=response)
        if response == "":
            return QueryResponse(server=None)
        else:
            return toReturn


    def JoinReplica(self, request: JoinRequest, context) -> JoinReplicaResponse:
        return JoinReplicaResponse(role=self.shard_master_service.join_replica(request.server))


    def QueryReplica(self, request: QueryReplicaRequest, context) -> QueryResponse:
        return QueryResponse(server=self.shard_master_service.query_replica(request.key, request.operation))
