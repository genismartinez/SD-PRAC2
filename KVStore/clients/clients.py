from typing import Union, Dict
import grpc
import logging
from KVStore.protos.kv_store_pb2 import GetRequest, PutRequest, GetResponse, AppendRequest
from KVStore.protos.kv_store_pb2_grpc import KVStoreStub
from KVStore.protos.kv_store_shardmaster_pb2 import QueryRequest, QueryResponse, QueryReplicaRequest, Operation
from KVStore.protos.kv_store_shardmaster_pb2_grpc import ShardMasterStub

logger = logging.getLogger(__name__)


def _get_return(ret: GetResponse) -> Union[str, None]:
    if ret.HasField("value"):
        return ret.value
    else:
        return None

class SimpleClient:
    def __init__(self, kvstore_address: str):
        self.channel = grpc.insecure_channel(kvstore_address)
        self.stub = KVStoreStub(self.channel)

    def get(self, key: int) -> Union[str, None]:
        get_request = GetRequest(key=key)
        return _get_return(self.stub.Get(get_request))

    def l_pop(self, key: int) -> Union[str, None]:
        get_request = GetRequest(key=key)
        return _get_return(self.stub.LPop(get_request))

    def r_pop(self, key: int) -> Union[str, None]:
        get_request = GetRequest(key=key)
        return _get_return(self.stub.RPop(get_request))

    def put(self, key: int, value: str):
        put_request = PutRequest(key=key, value=value)
        self.stub.Put(put_request)

    def append(self, key: int, value: str):
        print("CLIENT APPEND")
        append_request = AppendRequest(key=key, value=value)
        self.stub.Append(append_request)

    def stop(self):
        self.channel.close()


class ShardClient(SimpleClient):
    def __init__(self, shard_master_address: str):
        super().__init__(shard_master_address)
        self.channel = grpc.insecure_channel(shard_master_address)
        self.stub = ShardMasterStub(self.channel)
        self._servers = dict()  # dictionary that will store port:stub, so that overhead is reduced

    def _query(self , key: int):
        return self.stub.Query(QueryRequest(key=key))   # We request the port of the server that has the key

    def get(self, key: int) -> Union[str, None]:
        port = self._query(key)     # We request the server where is the key
        channel = grpc.insecure_channel(port)
        stub = KVStoreStub(channel)
        return stub.Get(GetRequest(key=key))

    def l_pop(self, key: int) -> Union[str, None]:
        port = self._query(key)
        channel = grpc.insecure_channel(port)
        stub = KVStoreStub(channel)
        return stub.LPop(GetRequest(key=key))

    def r_pop(self, key: int) -> Union[str, None]:
        port = self._query(key)
        channel = grpc.insecure_channel(port)
        stub = KVStoreStub(channel)
        return stub.RPop(GetRequest(key=key))

    def put(self, key: int, value: str):
        port = self._query(key)
        channel = grpc.insecure_channel(port)
        stub = KVStoreStub(channel)
        stub.Put(PutRequest(key=key, value=value))

    def append(self, key: int, value: str):
        port = self._query(key)
        channel = grpc.insecure_channel(port)
        stub = KVStoreStub(channel)
        stub.Append(AppendRequest(key=key, value=value))


class ShardReplicaClient(ShardClient):

    def get(self, key: int) -> Union[str, None]:
        """
        To fill with your code
        """

    def l_pop(self, key: int) -> Union[str, None]:
        """
        To fill with your code
        """


    def r_pop(self, key: int) -> Union[str, None]:
        """
        To fill with your code
        """


    def put(self, key: int, value: str):
        """
        To fill with your code
        """


    def append(self, key: int, value: str):
        """
        To fill with your code
        """

