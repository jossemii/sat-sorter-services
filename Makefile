# onnx
python3 -m grpc_tools.protoc -I. --python_out=proto onnx.proto --experimental_allow_proto3_optional &&
# api
python3 -m grpc_tools.protoc -I. --python_out=proto --grpc_python_out=proto api.proto --experimental_allow_proto3_optional &&
# gateway
python3 -m grpc_tools.protoc -I. --python_out=proto --grpc_python_out=proto gateway.proto --experimental_allow_proto3_optional &&
# regresion
python3 -m grpc_tools.protoc -I. --python_out=proto --grpc_python_out=proto regresion.proto --experimental_allow_proto3_optional &&
# solvers_dataset
python3 -m grpc_tools.protoc -I. --python_out=proto solvers_dataset.proto --experimental_allow_proto3_optional

# Pyarmor
pyarmor pack start.py
mv dist .service