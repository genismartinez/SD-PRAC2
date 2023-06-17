import time
import random
import uuid
from typing import Dict, Union, List
import logging
import grpc
from KVStore.protos.kv_store_pb2 import *
from KVStore.protos.kv_store_pb2_grpc import KVStoreServicer, KVStoreStub
from google.protobuf import empty_pb2 as google_dot_protobuf_dot_empty__pb2
from multiprocessing import Manager
from typing import Optional

from KVStore.protos.kv_store_shardmaster_pb2 import Role, QueryRequest

EVENTUAL_CONSISTENCY_INTERVAL: int = 2

logger = logging.getLogger("KVStore")


class KVStorageService:
    def __init__(self):
        self.manager = Manager()
        self.data: Dict[int, str] = self.manager.dict()
        self.replicas: Dict[str, KVStoreStub] = self.manager.dict()

    def get(self, key: int) -> Optional[str]:
        print(f"GET: {key} -> {self.data.get(key)}")
        return self.data.get(key)

    def l_pop(self, key: int) -> Optional[str]:
        if key not in self.data:
            return None
        value = self.data[key]
        if len(value) > 0:
            popped_char = value[0]
            self.data[key] = value[1:]
            return popped_char
        return ""

    def r_pop(self, key: int) -> Optional[str]:
        if key not in self.data:
            return None
        value = self.data[key]
        if len(value) > 0:
            popped_char = value[-1]
            self.data[key] = value[:-1]
            return popped_char
        return ""

    def put(self, key: int, value: str):
        self.data[key] = value
        print(f"PUT: {key} -> {value}")

    def append(self, key: int, value: str):
        if key not in self.data:
            self.data[key] = value
            print(f"APPEND: {key} -> {value}")
        else:
            self.data[key] = self.data[key] + value
            print(f"APPEND: {key} -> {self.data[key]} -> {value}")

    def redistribute(self, destination_server: str, lower_val: int, upper_val: int):
        keys_values = [KeyValue(key=key, value=self.data[key]) for key in self.data.keys() if lower_val <= key <= upper_val]
        self.data = {key: value for key, value in self.data.items() if key not in keys_values}

        if keys_values:
            stub = KVStoreStub(grpc.insecure_channel(destination_server))
            stub.Transfer(TransferRequest(keys_values=keys_values))

    def transfer(self, keys_values: List[KeyValue]):
        self.data.update({element.key: element.value for element in keys_values})

    def add_replica(self, server: str):
        self.replicas[server] = KVStoreStub(grpc.insecure_channel(server))

    def remove_replica(self, server: str):
        del self.replicas[server]

class KVStorageSimpleService(KVStorageService):

    def __init__(self):
        super().__init__()
        self.lower_bound = 0
        self.upper_bound = 100
        self._dictionary = dict()
        self._locks = dict()    # lock = 1 -> franja crítica NO ocupada
                                # lock = 0 -> franja crítica ocupada
                                # wait -> while lock == 0: wait then, lock = 1
                                # signal -> while lock == 1: signal then, lock = 0

    def waitKey(self, key: int):
        identifier = uuid.uuid4()  # Generamos un identificador único para cada petición
        if key in self._locks:  # Si el identificador ya está en la lista de locks, no se puede acceder
            self._locks[key].append(identifier)  # Añadimos el identificador a la lista de locks
        else:
            self._locks[key] = [identifier]  # Creamos una nueva lista de locks con el identificador
        print("PROCESO: " + str(identifier) + " COLA ESPERA DE KEY: " + str(self._locks[key]) + " KEY: " + str(key))
        while self._locks[key][0] != identifier:  # Mientras no sea el primer identificador de la lista
            print("PROCESO: " + str(identifier) + " ESPERANDO A QUE SE LIBERE LA KEY: ")
            time.sleep(1)  # Esperamos activamente == wait

    def signalKey(self, key: int):
        self._locks[key].pop(0)  # Eliminamos el identificador de la lista de locks == signal

    def get(self, key: int) -> Union[str, None]:
        self.waitKey(key)  # Esperamos activamente a que la key esté libre == wait
        if key in self._dictionary:  # Si la key está en el diccionario
            temp = self._dictionary[key]  # Guardamos el valor de la key temporalmente
        else:
            temp = None  # Si no está, devolvemos None
        self.signalKey(key)  # Liberamos la key == signal
        return temp  # Devolvemos el valor de la key



    def l_pop(self, key: int) -> Union[str, None]:
        try:
            self.waitKey(key)   # Esperamos activamente == wait
            if len(self._dictionary[key]) > 1:
                char = self._dictionary[key][0]
                self._dictionary[key] = self._dictionary[key][1:]
            elif len(self._dictionary[key]) == 1:
                char = self._dictionary[key][0]
                self._dictionary[key] = ""
            else:
                char = ""
            self.signalKey(key)  # Liberamos la key == signal
            return char
        except KeyError:
            return None

    def r_pop(self, key: int) -> Union[str, None]:
        try:
            self.waitKey(key)   # Esperamos activamente == wait
            if len(self._dictionary[key]) > 1:
                char = self._dictionary[key][-1]
                self._dictionary[key] = self._dictionary[key][:-1]
            elif len(self._dictionary[key]) == 1:
                char = self._dictionary[key][0]
                self._dictionary[key] = ""
            else:
                char = ""
            self.signalKey(key)  # Liberamos la key == signal
            return char
        except KeyError:
            return None

    # Saves the value into the key. If the key already exists, it overwrites it.
    def put(self, key: int, value: str):
        self.waitKey(key)   # Esperamos activamente == wait
        self._dictionary[key] = value
        self.signalKey(key)  # Liberamos la key == signal

    #   Concateates the specified value to the leftmost end of the value in the key.
    #   If the key does not exist, it saves the value into the key.
    def append(self, key: int, value: str):
        print("APPEND: " + str(key) + " " + str(value))
        self.waitKey(key)  # Esperamos activamente == wait
        if key in self._dictionary:  # Si la key ya existe, concatenamos el valor
            self._dictionary[key] = self._dictionary[key] + value
        else:
            self._dictionary[key] = value
        print("DICCIONARIO: " + str(self._dictionary[key]))
        self.signalKey(key)  # Liberamos la key == signal

    def redistribute(self, destination_server: str, lower_val: int, upper_val: int):
        """
        # We build the dictionary with the matching keys
        keys_values = dict()
        lista = list[KeyValue]
        for key in range(lower_val, upper_val):
            if key in self._dictionary:
                lista.append(KeyValue(key=key, value=self._dictionary[key]))

                #keys_values[key] = self._dictionary[key]  # We add the key and its value to the dictionary
        # We send the dictionary to the destination server

        channel = grpc.insecure_channel(destination_server)
        stub = KVStoreStub(channel)
        # Generate gRPC request to TransferRequest
        stub.Transfer(TransferRequest(keys_values=lista))
        """
        keys_values = [KeyValue(key=key, value=self._dictionary[key]) for key in self._dictionary.keys() if lower_val <= key <= upper_val]
        self._dictionary = {key: value for key, value in self._dictionary.items() if key not in keys_values}

        if keys_values:
            stub = KVStoreStub(grpc.insecure_channel(destination_server))
            stub.Transfer(TransferRequest(keys_values=keys_values))

    def transfer(self, keys_values: List[KeyValue]):
        self._dictionary.update({element.key: element.value for element in keys_values})


class KVStorageReplicasService(KVStorageSimpleService):
    role: Role

    def __init__(self, consistency_level: int):
        super().__init__()
        self.consistency_level = consistency_level
        """
        To fill with your code
        """

    def l_pop(self, key: int) -> str:
        """
        To fill with your code
        """

    def r_pop(self, key: int) -> str:
        """
        To fill with your code
        """

    def put(self, key: int, value: str):
        """
        To fill with your code
        """

    def append(self, key: int, value: str):
        """
        To fill with your code
        """

    def add_replica(self, server: str):
        """
        To fill with your code
        """

    def remove_replica(self, server: str):
        """
        To fill with your code
        """

    def set_role(self, role: Role):
        logger.info(f"Got role {role}")
        self.role = role


class KVStorageServicer(KVStoreServicer):

    def __init__(self, service: KVStorageService):
        self.storage_service = service
        """
        To fill with your code
        """

    def Get(self, request: GetRequest, context) -> GetResponse:
        key = request.key
        response = self.storage_service.get(key)
        get_response = GetResponse()
        if response is not None:
            get_response.value = response
        return get_response

    def LPop(self, request: GetRequest, context) -> GetResponse:
        key = request.key
        response = self.storage_service.l_pop(key)
        get_response = GetResponse()
        if response is not None:
            get_response.value = response
        return get_response

    def RPop(self, request: GetRequest, context) -> GetResponse:
        key = request.key
        response = self.storage_service.r_pop(key)
        get_response = GetResponse()
        if response is not None:
            get_response.value = response
        return get_response

    def Put(self, request: PutRequest, context) -> google_dot_protobuf_dot_empty__pb2.Empty:
        key = request.key
        value = request.value
        self.storage_service.put(key, value)
        return google_dot_protobuf_dot_empty__pb2.Empty()

    def Append(self, request: AppendRequest, context) -> google_dot_protobuf_dot_empty__pb2.Empty:
        key = request.key
        value = request.value
        self.storage_service.append(key, value)
        return google_dot_protobuf_dot_empty__pb2.Empty()

    def Redistribute(self, request: RedistributeRequest, context) -> google_dot_protobuf_dot_empty__pb2.Empty:
        destination_server = request.destination_server
        lower_val = request.lower_val
        upper_val = request.upper_val
        self.storage_service.redistribute(destination_server, lower_val, upper_val)
        return google_dot_protobuf_dot_empty__pb2.Empty()

    def Transfer(self, request: TransferRequest, context) -> google_dot_protobuf_dot_empty__pb2.Empty:
        keys_values = request.keys_values
        self.storage_service.transfer(list(keys_values))
        return google_dot_protobuf_dot_empty__pb2.Empty()

    def AddReplica(self, request: ServerRequest, context) -> google_dot_protobuf_dot_empty__pb2.Empty:
        server = request.server
        self.storage_service.add_replica(server)
        return google_dot_protobuf_dot_empty__pb2.Empty()

    def RemoveReplica(self, request: ServerRequest, context) -> google_dot_protobuf_dot_empty__pb2.Empty:
        server = request.server
        self.storage_service.remove_replica(server)
        return google_dot_protobuf_dot_empty__pb2.Empty()
