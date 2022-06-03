FROM python:3.8
RUN apt-get update --fix-missing && \
  pip3 install grpcio protobuf==3.19.4 onnxruntime iterators

COPY dist /satsorter/
