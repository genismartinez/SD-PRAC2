import random
import string
import time
import logging
from multiprocessing import Process

from KVStore.clients.clients import SimpleClient

logger = logging.getLogger(__name__)

TIMEOUT = 2
WAIT = 1
RETRIES = 1

KB = 1024
WAIT_TIME = 0.5

SHARDMASTER_PORT = 52003
STORAGE_PORTS = [52004 + i for i in range(100)]

KEYS_LOWER_THRESHOLD = 0
KEYS_UPPER_THRESHOLD = 100

def get_port():
    return STORAGE_PORTS.pop(random.randrange(len(STORAGE_PORTS)))


def wait(wait_time: int = WAIT_TIME):
    time.sleep(wait_time)


def gen_data(N: int):
    return ''.join(random.choices(string.ascii_uppercase +
                                  string.digits, k=int(N * KB)))


def test_get(client: SimpleClient, key: int, expected_value: str) -> bool:
    def _get():
        try:
            value = client.get(key)
        except Exception as e:
            print(e)
            return False

        if expected_value is not None:
            return value == expected_value
        else:
            return value is None

    for _ in range(RETRIES):
        result: bool = _get()
        if result is True:
            return True
        time.sleep(WAIT)
    return False


def test_l_pop(client: SimpleClient, key: int, expected_value: str) -> bool:
    def _l_pop():
        try:
            value = client.l_pop(key)
        except Exception as e:
            print(e)
            return False

        if expected_value is not None:
            return value == expected_value
        else:
            return value is None

    for _ in range(RETRIES):
        result: bool = _l_pop()
        if result is True:
            return True
        time.sleep(WAIT)
    return False


def test_r_pop(client: SimpleClient, key: int, expected_value: str) -> bool:
    def _r_pop():
        try:
            value = client.r_pop(key)
        except Exception as e:
            print(e)
            return False

        if expected_value is not None:
            return value == expected_value
        else:
            return value is None

    for _ in range(RETRIES):
        result: bool = _r_pop()
        if result is True:
            return True
        time.sleep(WAIT)
    return False


def test_put(client: SimpleClient, key: int, value: str) -> bool:
    def _put():
        try:
            client.put(key, value)
            return True
        except Exception as e:
            print(e)
            return False

    for _ in range(RETRIES):
        result: bool = _put()
        if result is True:
            return True
        time.sleep(WAIT)
    return False


def test_append(client: SimpleClient, key: int, value: str) -> bool:
    def _append():
        try:
            client.append(key, value)
            return True
        except Exception as e:
            print(e)
            return False

    for _ in range(RETRIES):
        result: bool = _append()
        if result is True:
            return True
        time.sleep(WAIT)
    return False


def test_put(client: SimpleClient, key: int, value: str) -> bool:
    def _put():
        try:
            client.put(key, value)
            return True
        except Exception as e:
            print(e)
            return False

    for _ in range(RETRIES):
        result: bool = _put()
        if result is True:
            return True
        time.sleep(WAIT)
    return False


class Test:
    def __init__(self, master_adress: str, num_clients: int):
        self.master_address = master_adress
        self.num_clients = num_clients

    def _test(self, client_id: int):
        pass

    def test(self):
        procs = [Process(target=self._test, args=[client_id]) for client_id in range(self.num_clients)]
        [proc.start() for proc in procs]
        [proc.join() for proc in procs]
