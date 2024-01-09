FROM python:3.11
RUN apt-get update --fix-missing && \
  pip3 install git+https://github.com/celaut-project/grpc-bb \
    git+https://github.com/celaut-project/node-driver \
    numpy>=1.16.6 scikit-learn skl2onnx
