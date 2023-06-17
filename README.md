# Sharded Key-value Storage System
###  Distributed Systems course: Assignment 2
###  Universitat Rovira i Virgili

# Installation
· Linux
```bash
python3 -m pip install -r requirements.txt
python3 -m grpc_tools.protoc --proto_path=. --grpc_python_out=. --pyi_out=. --python_out=. ./KVStore/protos/*.proto
python3 -m pip install -e .
```
· Windows
```bash
py -m pip install -r requirements.txt
py -m grpc_tools.protoc --proto_path=. --grpc_python_out=. --pyi_out=. --python_out=. ./KVStore/protos/*.proto
py -m pip install -e .
```

# Evaluation
## First subtask (simple KV storage)
```bash
python3 eval/single_node_storage.py
```

## Second subtask (sharded KV storage)
```bash
python3 eval/sharded.py
```

## Third subtask (sharded KV storage with replica groups)
```bash
python3 eval/replicas.py
```
