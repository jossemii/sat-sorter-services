FROM python:3.11
RUN apt-get update --fix-missing && \
    pip3 install git+https://github.com/bee-rpc-protocol/bee-rpc \
    git+https://github.com/celaut-project/libraries
COPY service /regresioncnf
