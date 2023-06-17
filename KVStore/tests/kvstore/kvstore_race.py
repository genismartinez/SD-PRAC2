from multiprocessing import Process, Queue
import logging
from KVStore.clients.clients import SimpleClient
from KVStore.logger import setup_logger
from KVStore.tests.utils import test_get, test_append, Test

"""
Tests on parallel storage requests in a race condition on a single storage server.
"""

logger = logging.getLogger(__name__)
DATA = "ORA "
NUM_REQUESTS = 5


class SimpleKVStoreRaceTests(Test):

    def _test(self, client_id: int):
        setup_logger()

        client = SimpleClient(self.master_address)
        for _ in range(NUM_REQUESTS):
            test_append(client, 15, DATA)
        client.stop()

    def check_result(self, _queue):
        client = SimpleClient(self.master_address)
        expected_result = DATA * NUM_REQUESTS * self.num_clients
        res = test_get(client, 15, expected_result)
        _queue.put(res)

    def test(self):
        super().test()

        queue = Queue()

        proc = Process(target=self.check_result, args=(queue,))
        proc.start()
        proc.join()
        assert (queue.get() is True)
