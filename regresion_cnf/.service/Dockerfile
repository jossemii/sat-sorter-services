FROM python:3.11
RUN apt-get update --fix-missing && \
  pip3 install git+https://github.com/pee-rpc-protocol/pee-rpc \
    git+https://github.com/celaut-project/node-driver \
    numpy>=1.16.6 scikit-learn==1.4.1.post1 skl2onnx==1.16.0
