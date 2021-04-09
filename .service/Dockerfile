FROM ubuntu
RUN apt-get update --fix-missing && apt-get -y install python3.6 python3-pip && apt-get clean
RUN pip3 install grpcio protobuf onnxruntime 

# For regression service.
RUN export DEBIAN_FRONTEND=noninteractive && apt-get update --fix-missing && apt-get -yq install git cmake && apt-get clean
RUN git clone https://github.com/onnx/onnx.git
RUN cd onnx && git submodule update --init --recursive
RUN python3 onnx/setup.py install
RUN apt-get -y install libprotobuf-dev protobuf-compiler
RUN pip3 install cython protobuf numpy onnx scikit-learn skl2onnx

RUN mkdir /satrainer && cd /satrainer && apt-get update && apt-get -y install git && git clone --branch proto https://github.com/josemibnf/SATrainer.git && mv SATrainer/* . && rm -rf SATrainer  && rm -rf .git
