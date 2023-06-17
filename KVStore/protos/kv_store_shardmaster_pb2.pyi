from google.protobuf import empty_pb2 as _empty_pb2
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

APPEND: Operation
DESCRIPTOR: _descriptor.FileDescriptor
GET: Operation
L_POP: Operation
MASTER: Role
PUT: Operation
REPLICA: Role
R_POP: Operation

class JoinReplicaResponse(_message.Message):
    __slots__ = ["role"]
    ROLE_FIELD_NUMBER: _ClassVar[int]
    role: Role
    def __init__(self, role: _Optional[_Union[Role, str]] = ...) -> None: ...

class JoinRequest(_message.Message):
    __slots__ = ["server"]
    SERVER_FIELD_NUMBER: _ClassVar[int]
    server: str
    def __init__(self, server: _Optional[str] = ...) -> None: ...

class LeaveRequest(_message.Message):
    __slots__ = ["server"]
    SERVER_FIELD_NUMBER: _ClassVar[int]
    server: str
    def __init__(self, server: _Optional[str] = ...) -> None: ...

class QueryReplicaRequest(_message.Message):
    __slots__ = ["key", "operation"]
    KEY_FIELD_NUMBER: _ClassVar[int]
    OPERATION_FIELD_NUMBER: _ClassVar[int]
    key: int
    operation: Operation
    def __init__(self, key: _Optional[int] = ..., operation: _Optional[_Union[Operation, str]] = ...) -> None: ...

class QueryRequest(_message.Message):
    __slots__ = ["key"]
    KEY_FIELD_NUMBER: _ClassVar[int]
    key: int
    def __init__(self, key: _Optional[int] = ...) -> None: ...

class QueryResponse(_message.Message):
    __slots__ = ["server"]
    SERVER_FIELD_NUMBER: _ClassVar[int]
    server: str
    def __init__(self, server: _Optional[str] = ...) -> None: ...

class Role(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = []

class Operation(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = []
