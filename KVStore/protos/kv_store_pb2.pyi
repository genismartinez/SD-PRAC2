from google.protobuf import empty_pb2 as _empty_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class AppendRequest(_message.Message):
    __slots__ = ["key", "value"]
    KEY_FIELD_NUMBER: _ClassVar[int]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    key: int
    value: str
    def __init__(self, key: _Optional[int] = ..., value: _Optional[str] = ...) -> None: ...

class GetRequest(_message.Message):
    __slots__ = ["key"]
    KEY_FIELD_NUMBER: _ClassVar[int]
    key: int
    def __init__(self, key: _Optional[int] = ...) -> None: ...

class GetResponse(_message.Message):
    __slots__ = ["value"]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    value: str
    def __init__(self, value: _Optional[str] = ...) -> None: ...

class KeyValue(_message.Message):
    __slots__ = ["key", "value"]
    KEY_FIELD_NUMBER: _ClassVar[int]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    key: int
    value: str
    def __init__(self, key: _Optional[int] = ..., value: _Optional[str] = ...) -> None: ...

class PutRequest(_message.Message):
    __slots__ = ["key", "value"]
    KEY_FIELD_NUMBER: _ClassVar[int]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    key: int
    value: str
    def __init__(self, key: _Optional[int] = ..., value: _Optional[str] = ...) -> None: ...

class RedistributeRequest(_message.Message):
    __slots__ = ["destination_server", "lower_val", "upper_val"]
    DESTINATION_SERVER_FIELD_NUMBER: _ClassVar[int]
    LOWER_VAL_FIELD_NUMBER: _ClassVar[int]
    UPPER_VAL_FIELD_NUMBER: _ClassVar[int]
    destination_server: str
    lower_val: int
    upper_val: int
    def __init__(self, destination_server: _Optional[str] = ..., lower_val: _Optional[int] = ..., upper_val: _Optional[int] = ...) -> None: ...

class ServerRequest(_message.Message):
    __slots__ = ["server"]
    SERVER_FIELD_NUMBER: _ClassVar[int]
    server: str
    def __init__(self, server: _Optional[str] = ...) -> None: ...

class TransferRequest(_message.Message):
    __slots__ = ["keys_values"]
    KEYS_VALUES_FIELD_NUMBER: _ClassVar[int]
    keys_values: _containers.RepeatedCompositeFieldContainer[KeyValue]
    def __init__(self, keys_values: _Optional[_Iterable[_Union[KeyValue, _Mapping]]] = ...) -> None: ...
