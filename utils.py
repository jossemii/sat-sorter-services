CHUNK_SIZE = 1024 * 1024  # 1MB
import os
import gateway_pb2
from random import randint
from typing import Generator
from threading import Condition

class Signal():
    # The parser use change() when reads a signal on the buffer.
    # The serializer use wait() for stop to send the buffer if it've to do it.
    # It's thread safe because the open var is only used by one thread (the parser) with the change method.
    def __init__(self, exist: bool = True) -> None:
        self.exist = exist
        if exist: self.open = True
        if exist: self.condition = Condition()
    
    def change(self):
        if self.exist:
            if self.open:
                self.open = False  # Stop the input buffer.
            else:
                self.condition.notify_all()
                self.open = True  # Continue the input buffer.
    
    def wait(self):
        if self.exist and not self.open:
            self.condition.wait()

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

def parse_from_buffer(request_iterator, message_field = None, signal = Signal(exist=False)):
    while True:
        all_buffer = bytes('', encoding='utf-8')
        while True:
            buffer = next(request_iterator)
            if buffer.HasField('separator'):
                break
            if buffer.HasField('signal'):
                signal.change()
                continue
            if buffer.HasField('chunk'):
                all_buffer += buffer.chunk
        if message_field: 
            message = message_field()
            message.ParseFromString(
                all_buffer
            )
            yield message
        else:
            yield all_buffer # Clean buffer index bytes.

def serialize_to_buffer(message_iterator, signal = Signal(exist=False)):
    if not hasattr(message_iterator, '__iter__'): message_iterator=[message_iterator]
    for message in message_iterator:
        message_bytes = message.SerializeToString()
        if len(message_bytes) < CHUNK_SIZE:
            signal.wait()
            yield gateway_pb2.Buffer(
                chunk = bytes(message_bytes)
            )
        else:
            try:
                byte_list = list(message_bytes)
                for chunk in [byte_list[i:i + CHUNK_SIZE] for i in range(0, len(byte_list), CHUNK_SIZE)]:
                    signal.wait()
                    yield gateway_pb2.Buffer(
                                    chunk = bytes(chunk)
                                )
            except: # INEFICIENT.
                file =  os.path.abspath(os.curdir) + '/__hycache__/' + str(len(message_bytes)) + ':' + str(randint(1,999))
                open(file, 'wb').write(message_bytes)
                try:
                    for b in get_file_chunks(file): 
                        signal.wait()
                        yield b
                finally:
                    os.remove(file)

        yield gateway_pb2.Buffer(
            separator = bytes('', encoding='utf-8')
        )

def client_grpc(method, output_field = None, input=None, timeout=None):
    signal = Signal()
    for b in parse_from_buffer(
        request_iterator = method(
                            serialize_to_buffer(
                                input if input else '',
                                signal = signal
                            ),
                            timeout = timeout
                        ),
        message_field = output_field,
        signal = signal
    ): yield b