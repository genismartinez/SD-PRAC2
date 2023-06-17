from KVStore.clients.clients import SimpleClient
from KVStore.logger import setup_logger
from KVStore.tests.utils import test_get, test_put, test_append, test_l_pop, test_r_pop, Test
import logging

logger = logging.getLogger(__name__)


class SimpleKVStoreTests(Test):

    def _test(self, client_id: int):

        setup_logger()

        client = SimpleClient(self.master_address)

        assert (test_get(client, 10, None))

        assert (test_put(client, 33, "?!?!?"))
        assert (test_get(client, 33, "?!?!?"))

        assert (test_append(client, 45, "huh?"))
        assert (test_get(client, 45, "huh?"))
        assert (test_put(client, 45, "huh!"))
        assert (test_get(client, 45, "huh!"))
        assert (test_append(client, 45, "?"))
        assert (test_get(client, 45, "huh!?"))

        assert (test_l_pop(client, 3, None))
        assert (test_l_pop(client, 45, "h"))
        assert (test_r_pop(client, 45, "?"))
        assert (test_l_pop(client, 45, "u"))
        assert (test_r_pop(client, 45, "!"))
        assert (test_r_pop(client, 45, "h"))
        assert (test_l_pop(client, 45, ""))

        assert (test_get(client, 86, None))
        assert (test_get(client, 34, None))
        assert (test_append(client, 86, "URV_ROCKS"))
        assert (test_append(client, 34, "paxos_enjoyer"))
        assert (test_get(client, 86, "URV_ROCKS"))
        assert (test_get(client, 34, "paxos_enjoyer"))


