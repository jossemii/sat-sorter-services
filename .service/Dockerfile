FROM ubuntu
RUN apt-get update --fix-missing && \
  apt-get -y install python3.6 python3-pip && \
  apt-get clean && \
  pip3 install grpcio protobuf onnxruntime 

# For regression service.
RUN export DEBIAN_FRONTEND=noninteractive && \
  apt-get update --fix-missing && \
  apt-get -y install cmake protobuf-compiler libprotoc-dev && \
  pip3 install sklearn protobuf numpy scikit-learn skl2onnx

RUN mkdir /satsorter && cd /satsorter && \
  apt-get update && apt-get -y install git && \
  git clone --branch proto https://github.com/josemibnf/SATsorter.git && mv SATsorter/* . && \
  rm -rf SATsorter  && rm -rf .git
