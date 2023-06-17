from concurrent import futures
from multiprocessing import Process, Queue
import grpc
from KVStore.kvstorage.kvstorage import KVStorageServicer, KVStorageReplicasService
from KVStore.logger import setup_logger
from KVStore.protos import kv_store_pb2_grpc, kv_store_shardmaster_pb2_grpc
from KVStore.protos.kv_store_shardmaster_pb2 import JoinRequest, LeaveRequest, JoinReplicaResponse
from KVStore.tests.utils import wait

HOSTNAME: str = "localhost"


def _run(end_queue: Queue, storage_server_port: int, shardmaster_port: int, consistency_level: int):

    setup_logger()

    address: str = "%s:%d" % (HOSTNAME, storage_server_port)

    storage_server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    service = KVStorageReplicasService(consistency_level)
    servicer = KVStorageServicer(service)
    kv_store_pb2_grpc.add_KVStoreServicer_to_server(servicer, storage_server)

    print("KV Storage server listening on: %s" % address)
    storage_server.add_insecure_port(address)
    storage_server.start()

    # open a gRPC channel
    channel = grpc.insecure_channel(f'localhost:{shardmaster_port}')
    # create a stub (client)
    stub = kv_store_shardmaster_pb2_grpc.ShardMasterStub(channel)
    req = JoinRequest(server=address)
    resp: JoinReplicaResponse = stub.JoinReplica(req)

    service.set_role(resp.role)

    try:
        end_queue.get(block=True, timeout=240)
        req = LeaveRequest(server=address)
        stub.Leave(req)

        wait(1)
        channel.close()

        storage_server.stop(0)

    except KeyboardInterrupt:
        storage_server.stop(0)
    except EOFError:
        storage_server.stop(0)


def run(port: int, shardmaster_port: int, consistency_level: int) -> Queue:
    end_queue = Queue()
    server_proc = Process(target=_run, args=[end_queue, port, shardmaster_port, consistency_level])
    server_proc.start()
    wait(0.5)
    return end_queue
