from datetime import time
import gateway_pb2
from typing import Generator

CHUNK_SIZE = 1024 * 1024  # 1MB

def get_file_chunks(filename) -> Generator[gateway_pb2.Buffer, None, None]:
    with open(filename, 'rb') as f:
        while True:
            piece = f.read(CHUNK_SIZE);
            if len(piece) == 0:
                return
            yield gateway_pb2.Buffer(chunk=piece)


def save_chunks_to_file(chunks: gateway_pb2.Buffer, filename):
    with open(filename, 'wb') as f:
        for buffer in chunks:
            f.write(buffer.chunk)

def parse_from_buffer(request_iterator, message_field = None) -> Generator:
    while True:
        all_buffer = ''
        for buffer in request_iterator:
            if buffer.separator:
                break
            all_buffer.append(buffer.chunk)
        
        if message_field: 
            message = message_field()
            message.ParseFromString(
                all_buffer
            )            
            yield message
        else:
            yield all_buffer # Clean buffer index bytes.

def serialize_to_buffer(message_iterator):
    if not hasattr(message_iterator, '__iter__'): message_iterator=[message_iterator]
    for message in message_iterator:
        for chunk in message.SerializeToString().read(CHUNK_SIZE):
            yield gateway_pb2.Buffer(
                chunk = chunk
            )
        yield gateway_pb2.Buffer(
            separator = ''
        )

def client_grpc(method, output_field = None, input=None, timeout=None) -> Generator:
    return parse_from_buffer(
        request_iterator = method(
                            serialize_to_buffer(
                                input if input else ''
                            ),
                            timeout = timeout
                        ),
        message_field = output_field
    )