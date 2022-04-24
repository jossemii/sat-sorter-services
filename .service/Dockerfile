FROM python:3.8
RUN apt-get update --fix-missing && \
  pip3 install grpcio protobuf onnxruntime iterators

COPY src /satsorter/
