from multiprocessing import Process

from KVStore.clients.clients import ShardClient
from KVStore.logger import setup_logger
from KVStore.tests.utils import test_get, test_append, Test

"""
Tests on simple storage requests on a single storage server.
"""

DATA = "MUDA "


class ShardkvAppendTests(Test):

    def _test(self, client_id: int, num_iter: int):
        setup_logger()

        client = ShardClient(self.master_address)
        assert (test_append(client, 81, DATA))
        assert (test_get(client, 81, DATA * (num_iter + 1)))
        client.stop()

    def test(self, num_iter: int):
        procs = [
            Process(target=self._test, args=[client_id, num_iter])
            for client_id in range(self.num_clients)
        ]
        [proc.start() for proc in procs]
        [proc.join() for proc in procs]

