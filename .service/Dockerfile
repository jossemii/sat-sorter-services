FROM ubuntu
RUN apt-get update --fix-missing && \
  apt-get -y install python3.6 python3-pip && \
  apt-get clean && \
  pip3 install grpcio protobuf onnxruntime iterators

RUN mkdir /satsorter && cd /satsorter && \
  apt-get update && apt-get -y install git && \
  git clone --branch proto https://github.com/josemibnf/SATsorter.git && mv SATsorter/* . && \
  mv SATsorter/.service/ . && rm -rf SATsorter  && rm -rf .git
