from concurrent import futures
from multiprocessing import Process
import grpc
from KVStore.logger import setup_logger
from KVStore.protos import kv_store_shardmaster_pb2_grpc
from KVStore.shardmaster.shardmaster import ShardMasterServicer, ShardMasterSimpleService
import logging

from KVStore.tests.utils import wait

logger = logging.getLogger(__name__)


HOSTNAME: str = "localhost"
def _run(port: int):

    setup_logger()

    address: str = "%s:%d" % (HOSTNAME, port)

    master_server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    servicer = ShardMasterServicer(ShardMasterSimpleService())
    kv_store_shardmaster_pb2_grpc.add_ShardMasterServicer_to_server(servicer, master_server)

    # listen on port 50051
    print("Shardmaster server listening on: %s" % address)
    master_server.add_insecure_port(address)
    master_server.start()

    try:
        while True:
            wait(1)
            logger.info("Shardmaster listening...")

    except KeyboardInterrupt:
        master_server.stop(0)
    except EOFError:
        master_server.stop(0)


def run(port: int) -> Process:
    server_proc = Process(target=_run, args=[port])
    server_proc.start()
    wait(0.5)
    return server_proc
