import logging
from KVStore.logger import setup_logger
import sys
sys.path.append("/Users/usuario/SD/SD-PRAC2/KVStore/tests/simple_shardkv")
from KVStore.shardmaster import start_shardmaster
from KVStore.kvstorage import start_storage_server_sharded
from KVStore.tests.sharded import ShardKVParallelTests, ShardKVSimpleTests, ShardkvAppendTests
from KVStore.tests.utils import wait, SHARDMASTER_PORT, get_port
from sys import platform

logger = logging.getLogger(__name__)

NUM_CLIENTS = 2
NUM_STORAGE_SERVERS = [2, 4, 8]
master_address = f"localhost:{SHARDMASTER_PORT}"


if __name__ ==  '__main__':

    if platform not in ["linux", "linux2"]:
        setup_logger()

    print("*************Sharded tests**************")

    print("Tests with changing shardmasters")
    for num_servers in NUM_STORAGE_SERVERS:
        print(f"{num_servers} storage servers.")
        server_proc = start_shardmaster.run(SHARDMASTER_PORT)

        storage_proc_end_queues = [start_storage_server_sharded.run(get_port(), SHARDMASTER_PORT) for i in
                                   range(num_servers)]

        test1 = ShardKVSimpleTests(master_address, 1)
        test1.test()

        test2 = ShardKVParallelTests(master_address, NUM_CLIENTS)
        test2.test()

        [queue.put(0) for queue in storage_proc_end_queues]
        wait()
        server_proc.terminate()
        wait()

    print("Tests redistributions 1")
    #  Test if the system supports dynamic removal of shards
    num_servers = 5
    server_proc = start_shardmaster.run(SHARDMASTER_PORT)

    storage_proc_end_queues = [
        start_storage_server_sharded.run(get_port(), SHARDMASTER_PORT)
        for i in range(num_servers)
    ]


    for i in range(num_servers - 1):
        print(f"{i} storage servers.")

        test2 = ShardKVParallelTests(master_address, NUM_CLIENTS)
        test2.test()
        storage_proc_end_queues[i].put(0)
        wait()

    storage_proc_end_queues[num_servers - 1].put(0)
    wait()
    server_proc.terminate()
    wait()

    print("Test redistribution 2 (keep data after redistribution)")
    # Test if data gets redistributed across shards when the number of nodes changes
    num_servers = 5
    server_proc = start_shardmaster.run(SHARDMASTER_PORT)

    storage_proc_end_queues = [
        start_storage_server_sharded.run(get_port(), SHARDMASTER_PORT) for i in
        range(num_servers)
    ]

    for i in range(num_servers - 1):
        print(f"{num_servers - i} storage servers.")

        test2 = ShardkvAppendTests(master_address, 1)
        test2.test(i)

        storage_proc_end_queues[0].put(0)
        wait()
        storage_proc_end_queues = storage_proc_end_queues[1:]

    for i in range(num_servers):
        print(f"{i + 1} storage servers.")

        test2 = ShardkvAppendTests(master_address, 1)
        test2.test(i + num_servers - 1)

        storage_proc_end_queues.append(start_storage_server_sharded.run(get_port(), SHARDMASTER_PORT))

    [queue.put(0) for queue in storage_proc_end_queues]
    wait()
    print("\n\n...Terminating server")
    server_proc.terminate()
