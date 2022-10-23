FROM python:3.8
RUN apt-get update --fix-missing && \
  pip3 install grpcio==1.48.1 protobuf==3.19.4 onnxruntime==1.12.1 iterators \
    git+https://github.com/jossemii/GRPCBigBuffer \
    git+https://github.com/jossemii/CelautFramework

COPY src /satsorter/
