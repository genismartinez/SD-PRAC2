import logging
from tabulate import tabulate
from KVStore.logger import setup_logger
from KVStore.shardmaster import start_shardmaster_replicas
from KVStore.kvstorage import start_storage_server_replicas
from KVStore.tests.replication.replication_performance import ShardKVReplicationPerformanceTest
from KVStore.tests.utils import wait, SHARDMASTER_PORT, get_port
from sys import platform

logger = logging.getLogger(__name__)

NUM_CLIENTS = 3
NUM_SHARDS = 2
NUM_STORAGE_SERVERS = 6
CONSISTENCY_LEVELS = [0, 1, 2]
master_address = f"localhost:{SHARDMASTER_PORT}"

if __name__ ==  '__main__':

    if platform not in ["linux", "linux2"]:
        setup_logger()

    print("*************Sharded + replicas tests**************")

    print("Testing throughput (OP/s) and number of errors for different consistency levels")
    print("Configuration:")
    print("\tNumber of clients: %d" % NUM_CLIENTS)
    print("\tNumber of shards: %d" % NUM_SHARDS)
    print("\tStorage servers: %d" % NUM_STORAGE_SERVERS)

    results = []

    for consistency_level in CONSISTENCY_LEVELS:

        print(f"Running with consistency level {consistency_level}.")

        server_proc = start_shardmaster_replicas.run(SHARDMASTER_PORT, NUM_SHARDS)

        storage_proc_end_queues = [
            start_storage_server_replicas.run(get_port(), SHARDMASTER_PORT, consistency_level)
            for i in range(NUM_STORAGE_SERVERS)
        ]

        test = ShardKVReplicationPerformanceTest(master_address, NUM_CLIENTS)
        throughput, error_rate, error_perc = test.test()
        results.append([consistency_level, throughput, error_rate, error_perc])

        storage_proc_end_queues.reverse()
        [queue.put(0) for queue in storage_proc_end_queues[:-NUM_SHARDS]]
        wait(1)
        [queue.put(0) for queue in storage_proc_end_queues[(NUM_STORAGE_SERVERS - NUM_SHARDS):]]

        wait(1)
        print("\n\n...Terminating server")
        server_proc.terminate()
        wait(1)

    print("Final results:")
    print("Configuration:")
    print("\tNumber of clients: %d" % NUM_CLIENTS)
    print("\tNumber of shards: %d" % NUM_SHARDS)
    print("\tStorage servers: %d" % NUM_STORAGE_SERVERS)
    # Show results in table
    print(tabulate(results, headers=['Consistency_level', 'Throughput (OP/s)', 'Error rate - Errors/s', '% of errors']))


