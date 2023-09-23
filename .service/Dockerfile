FROM python:3.11
RUN apt-get update --fix-missing && \
  pip3 install git+https://github.com/jossemii/grpc-bb \
    git+https://github.com/jossemii/node-driver \
    git+https://github.com/jossemii/resource-manager \
    onnxruntime==1.15.1 iterators==0.2.0
