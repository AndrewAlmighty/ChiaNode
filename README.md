To generate proto files:
python -m grpc_tools.protoc -I . --python_out=gen/ --grpc_python_out=gen/ controller.proto