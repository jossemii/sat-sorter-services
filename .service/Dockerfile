FROM python:3.8
RUN apt-get update --fix-missing && \
  pip3 install grpcio protobuf onnxruntime iterators

RUN mkdir /satsorter && cd /satsorter && \
  apt-get update && apt-get -y install git && \
  git clone --branch proto https://github.com/jossemii/SATsorter.git && mv SATsorter/* . && \
  mv SATsorter/.service/ . && rm -rf SATsorter  && rm -rf .git
