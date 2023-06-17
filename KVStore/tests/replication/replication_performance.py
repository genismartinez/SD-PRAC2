import multiprocessing
import random
import string
import time
import logging
from KVStore.clients.clients import ShardReplicaClient
from KVStore.logger import setup_logger
from KVStore.tests.utils import test_get, test_put, Test


logger = logging.getLogger(__name__)

"""
Tests on simple storage requests on a single storage server.
"""

EXEC_TIME = 8


class ShardKVReplicationPerformanceTest(Test):

    def _test(self, client_id: int, return_dict) -> (float, float):

        setup_logger()

        client = ShardReplicaClient(self.master_address)

        num_ops = 0
        num_errors = 0

        start_time = time.time()

        while time.time() - start_time < EXEC_TIME:

            key = random.randint(0, 100)
            value = ''.join(random.choices(string.ascii_uppercase +
                                           string.digits, k=4))

            test_put(client, key, value)
            res = test_get(client, key, value)

            num_ops += 1
            if res is False:
                logger.info("INCORRECT")
                num_errors += 1
            else:
                logger.info("CORRECT")

        return_dict[client_id] = (num_ops / EXEC_TIME, num_errors / EXEC_TIME, num_errors / num_ops )
        logger.info(f"{client_id}: {num_ops} ops, {num_errors} errors, {return_dict[client_id][1]} errors/s")

        client.stop()

    def test(self):

        manager = multiprocessing.Manager()
        return_dict = manager.dict()

        procs = [
            multiprocessing.Process(target=self._test, args=[client_id, return_dict])
            for client_id in range(self.num_clients)
        ]
        [p.start() for p in procs]
        [p.join() for p in procs]
        logger.info("Finished test")

        throughputs = [res[0] for res in return_dict.values()]
        error_rates = [res[1] for res in return_dict.values()]
        error_percs = [res[2] for res in return_dict.values()]

        return sum(throughputs), sum(error_rates), sum(error_percs)/len(error_percs)
