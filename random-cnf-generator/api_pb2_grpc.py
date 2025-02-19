# Generated by the gRPC Python protocol compiler plugin. DO NOT EDIT!
"""Client and server classes corresponding to protobuf-defined services."""
import grpc

from bee_rpc import buffer_pb2 as buffer__pb2


class RandomStub(object):
    """Missing associated documentation comment in .proto file."""

    def __init__(self, channel):
        """Constructor.

        Args:
            channel: A grpc.Channel.
        """
        self.RandomCnf = channel.stream_stream(
                '/api.Random/RandomCnf',
                request_serializer=buffer__pb2.Buffer.SerializeToString,
                response_deserializer=buffer__pb2.Buffer.FromString,
                )


class RandomServicer(object):
    """Missing associated documentation comment in .proto file."""

    def RandomCnf(self, request_iterator, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')


def add_RandomServicer_to_server(servicer, server):
    rpc_method_handlers = {
            'RandomCnf': grpc.stream_stream_rpc_method_handler(
                    servicer.RandomCnf,
                    request_deserializer=buffer__pb2.Buffer.FromString,
                    response_serializer=buffer__pb2.Buffer.SerializeToString,
            ),
    }
    generic_handler = grpc.method_handlers_generic_handler(
            'api.Random', rpc_method_handlers)
    server.add_generic_rpc_handlers((generic_handler,))


 # This class is part of an EXPERIMENTAL API.
class Random(object):
    """Missing associated documentation comment in .proto file."""

    @staticmethod
    def RandomCnf(request_iterator,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.stream_stream(request_iterator, target, '/api.Random/RandomCnf',
            buffer__pb2.Buffer.SerializeToString,
            buffer__pb2.Buffer.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)
