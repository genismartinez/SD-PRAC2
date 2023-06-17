import logging
import time
from concurrent import futures
import multiprocessing
import grpc
from KVStore.kvstorage.kvstorage import KVStorageServicer, KVStorageSimpleService
from KVStore.logger import setup_logger
from KVStore.protos import kv_store_pb2_grpc
from KVStore.tests.utils import wait

logger = logging.getLogger(__name__)

HOSTNAME: str = "localhost"


def _run(storage_server_port: int):

    setup_logger()

    address: str = "%s:%d" % (HOSTNAME, storage_server_port)

    storage_server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    logger.info("Created server")

    servicer = KVStorageServicer(KVStorageSimpleService())
    kv_store_pb2_grpc.add_KVStoreServicer_to_server(servicer, storage_server)
    logger.info("Created servicer")

    # listen on port 50051
    logger.info("KV Storage server listening on: %s" % address)
    storage_server.add_insecure_port(address)
    storage_server.start()

    try:
        time.sleep(3000)
    except KeyboardInterrupt:
        storage_server.stop(0)
    except EOFError:
        storage_server.stop(0)


def run(port: int):
    logger.info("Running server")

    server_proc = multiprocessing.Process(target=_run, args=[port, ])
    server_proc.start()

    wait(0.5)
    return server_proc
