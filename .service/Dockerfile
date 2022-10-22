FROM python:3.8
RUN apt-get update --fix-missing && \
  pip3 install grpcio protobuf==3.19.4 onnxruntime iterators \
    git+https://github.com/jossemii/GRPCBigBuffer \
    git+https://github.com/jossemii/CelautFramework

COPY src /satsorter/
