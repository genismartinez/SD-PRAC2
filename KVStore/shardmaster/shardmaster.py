import threading
import grpc
import google.protobuf.empty_pb2 as google_dot_protobuf_dot_empty__pb2
from multiprocessing import Manager
from KVStore.protos.kv_store_shardmaster_pb2_grpc import ShardMasterServicer
from KVStore.protos.kv_store_shardmaster_pb2 import *
from KVStore.protos.kv_store_pb2 import RedistributeRequest, ServerRequest
from KVStore.protos.kv_store_pb2_grpc import KVStoreStub

from typing import Dict, Tuple


#
def grpc_redistribute(source_server: str, destination_server: str, keys: Tuple[int, int]):
    redistribute_request = RedistributeRequest()    # Se crea el objeto de la clase RedistributeRequest
    redistribute_request.destination_server = destination_server    # Se asigna el server destino
    redistribute_request.lower_val = keys[0]    # Se asigna el valor inferior
    redistribute_request.upper_val = keys[1]    # Se asigna el valor superior
    print(f"Server origen: {source_server}, Server destino: {destination_server}, Keys: {keys}")
    with grpc.insecure_channel(source_server) as channel:   # Se crea el canal de comunicación con el server origen
        stub = KVStoreStub(channel) # Se crea el stub
        response = stub.Redistribute(redistribute_request)  # Se llama al método Redistribute del stub

    print(f"Llaves redistribuidas: {keys} desde {source_server} hasta {destination_server}")


class ShardMasterService:
    def __init__(self):
        self.manager = Manager()
        self.node_dict = self.manager.dict()
        self.servers = self.manager.list()
        self.lock = threading.Lock()

    def join(self, server: str):
        with self.lock: # Se bloquea el hilo hasta que se libere el lock
            if server not in self.servers:  # Si el server no está en la lista de servers
                self.servers.append(server)  # Se agrega el server a la lista de servers
                self.recalculate_shards()   # Se recalculan los shards
                self.redistribute_keys(server)  # Se redistribuyen las llaves
                print(f"Se ha unido -> ({server}), Servers -> {self.servers}")  # Se imprime el server que se ha unido
                print(f"Shards -> {self.node_dict}")    # Se imprime el diccionario de shards

    def leave(self, server: str):
        if server in self.servers:  # Si el server está en la lista de servers
            self.lock.acquire() # Bloquea el hilo hasta que se libere el lock
            try:
                self.servers.remove(server) # Se elimina el server de la lista de servers
                del self.node_dict[server]  # Se elimina el server del diccionario de shards
                if len(self.servers) >= 1:  # Si hay más de un server
                    self.recalculate_shards()   # Se recalculan los shards
                    destination = self.servers[0]   # Se asigna el primer server de la lista de servers
                    grpc_redistribute(server, destination, (0, 99)) # Se redistribuyen las llaves
                    self.redistribute_keys(server)  # Se redistribuyen las llaves
            finally:
                self.lock.release() # Libera el lock
            print(f"New Leave ({server}), Servers -> {self.servers}")
            print(f"Shards -> {self.node_dict}")

    def recalculate_shards(self):
        total_servers = len(self.servers)   # Se obtiene el total de servers
        total_keys = 100    # Se asigna el total de llaves

        if total_servers == 0:  # Si no hay servers
            return

        avg_keys_per_server = total_keys // total_servers   # Se calcula el promedio de llaves por server
        remaining_keys = total_keys % total_servers # Se calculan las llaves restantes

        lower_val = 0
        for i, server in enumerate(self.servers):   # Se itera sobre la lista de servers
            keys_count = avg_keys_per_server + (1 if i < remaining_keys else 0) # Se calcula el número de llaves
            upper_val = lower_val + keys_count - 1  # Se calcula el valor superior
            self.node_dict[server] = (lower_val, upper_val) # Se agrega el server al diccionario de shards
            lower_val = upper_val + 1   # Se actualiza el valor inferior

    def query(self, key):
        for address in self.servers:    # Se itera sobre la lista de servers
            print(f"Key: {str(key)} Range: {str(self.node_dict.get(address))} Server: {address}")   # Se imprime la llave, el rango y el server
            if self.node_dict.get(address)[0] <= key <= self.node_dict.get(address)[1]:   # Si la llave está en el rango del server
                return address  # Se retorna el server
        return None

    def redistribute_keys(self, server):
        if server in self.servers:  # Si el server está en la lista de servers
            server_index = self.servers.index(server)   # Se obtiene el índice del server
            prev_server = self.servers[server_index - 1] if server_index > 0 else None  # Se obtiene el server anterior
            next_server = self.servers[server_index + 1] if server_index < len(self.servers) - 1 else None  # Se obtiene el server siguiente

            if prev_server: # Si hay un server anterior
                shard = self.node_dict[prev_server] # Se obtiene el shard del server anterior
                if shard[1] != 99:  # Si el valor superior del shard es diferente de 99
                    destination = server    # Se asigna el server destino
                    keys = (shard[1] + 1, 99)   # Se asigna el rango de llaves
                    print(f"Redistributing keys: {keys} from {prev_server} to {destination}")
                    grpc_redistribute(prev_server, destination, keys)   # Se redistribuyen las llaves

            if next_server: # Si hay un server siguiente
                shard = self.node_dict[next_server] # Se obtiene el shard del server siguiente
                if shard[0] != 0:   # Si el valor inferior del shard es diferente de 0
                    destination = server    # Se asigna el server destino
                    keys = (0, shard[0])    # Se asigna el rango de llaves
                    print(f"Redistributing keys: {keys} from {next_server} to {destination}")
                    grpc_redistribute(next_server, destination, keys)   # Se redistribuyen las llaves
        else:
            print(f"Server {server} not in servers list")

    def join_replica(self, server: str) -> Role:
        pass

    def query_replica(self, key: int, op: Operation) -> str:
        pass


class ShardMasterSimpleService(ShardMasterService):
    def __init__(self):
        super().__init__()
        self.storage_service = KVStoreStub(grpc.insecure_channel("localhost:50051"))

    def join(self, server: str):
        super().join(server)

    def leave(self, server: str):
        super().leave(server)

    def query(self, key: int) -> str:
        return super().query(key)


class ShardMasterReplicasService(ShardMasterSimpleService):
    def __init__(self, number_of_shards: int):
        super().__init__()
        self.storage_service = ShardMasterSimpleService()   # Se crea un servicio de ShardMasterSimpleService
        """
        To fill with your code
        """

    def leave(self, server: str):
        super().leave(server)

    def join_replica(self, server: str) -> Role:
        respuesta = super().join_replica(server)
        if respuesta == "MASTER":
            return Role.MASTER
        elif respuesta == "REPLICA":
            return Role.REPLICA
        else:
            raise ValueError("Invalid role")

    def query_replica(self, key: int, op: Operation) -> str:
        response = super().query_replica(key, op)   # Se obtiene la respuesta del query
        return response


class ShardMasterServicer(ShardMasterServicer):
    def __init__(self, shard_master_service: ShardMasterService):
        self.shard_master_service = shard_master_service
        """
        To fill with your code
        """

    def Join(self, request: JoinRequest, context) -> google_dot_protobuf_dot_empty__pb2.Empty:
        self.shard_master_service.join(request.server)
        return google_dot_protobuf_dot_empty__pb2.Empty()

    def Leave(self, request: LeaveRequest, context) -> google_dot_protobuf_dot_empty__pb2.Empty:
        self.shard_master_service.leave(request.server)
        return google_dot_protobuf_dot_empty__pb2.Empty()

    def Query(self, request: QueryRequest, context) -> QueryResponse:
        response = self.shard_master_service.query(request.key)
        toReturn: QueryResponse = QueryResponse(server=response)
        if response == "":
            return QueryResponse(server=None)
        else:
            return toReturn

    def JoinReplica(self, request: JoinRequest, context) -> JoinReplicaResponse:
        return JoinReplicaResponse(role=self.shard_master_service.join_replica(request.server))

    def QueryReplica(self, request: QueryReplicaRequest, context) -> QueryResponse:
        return QueryResponse(server=self.shard_master_service.query_replica(request.key, request.operation))
