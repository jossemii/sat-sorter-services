FROM python:3.11
RUN pip3 install git+https://github.com/bee-rpc-protocol/bee-rpc \
    git+https://github.com/celaut-project/libraries \
    onnxruntime==1.15.0 iterators==0.2.0
COPY service /satsorter
