# Generated by the gRPC Python protocol compiler plugin. DO NOT EDIT!
"""Client and server classes corresponding to protobuf-defined services."""
import grpc

import instances_pb2 as instances__pb2


class ServiceStub(object):
    """Missing associated documentation comment in .proto file."""

    def __init__(self, channel):
        """Constructor.

        Args:
            channel: A grpc.Channel.
        """
        self.RandomCnf = channel.unary_unary(
                '/Service/RandomCnf',
                request_serializer=instances__pb2.WhoAreYourParams.SerializeToString,
                response_deserializer=instances__pb2.Cnf.FromString,
                )


class ServiceServicer(object):
    """Missing associated documentation comment in .proto file."""

    def RandomCnf(self, request, context):
        """port 8000
        """
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')


def add_ServiceServicer_to_server(servicer, server):
    rpc_method_handlers = {
            'RandomCnf': grpc.unary_unary_rpc_method_handler(
                    servicer.RandomCnf,
                    request_deserializer=instances__pb2.WhoAreYourParams.FromString,
                    response_serializer=instances__pb2.Cnf.SerializeToString,
            ),
    }
    generic_handler = grpc.method_handlers_generic_handler(
            'Service', rpc_method_handlers)
    server.add_generic_rpc_handlers((generic_handler,))


 # This class is part of an EXPERIMENTAL API.
class Service(object):
    """Missing associated documentation comment in .proto file."""

    @staticmethod
    def RandomCnf(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/Service/RandomCnf',
            instances__pb2.WhoAreYourParams.SerializeToString,
            instances__pb2.Cnf.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)


class SolverStub(object):
    """Missing associated documentation comment in .proto file."""

    def __init__(self, channel):
        """Constructor.

        Args:
            channel: A grpc.Channel.
        """
        self.Solve = channel.unary_unary(
                '/Solver/Solve',
                request_serializer=instances__pb2.Cnf.SerializeToString,
                response_deserializer=instances__pb2.Interpretation.FromString,
                )


class SolverServicer(object):
    """Missing associated documentation comment in .proto file."""

    def Solve(self, request, context):
        """port 8080
        """
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')


def add_SolverServicer_to_server(servicer, server):
    rpc_method_handlers = {
            'Solve': grpc.unary_unary_rpc_method_handler(
                    servicer.Solve,
                    request_deserializer=instances__pb2.Cnf.FromString,
                    response_serializer=instances__pb2.Interpretation.SerializeToString,
            ),
    }
    generic_handler = grpc.method_handlers_generic_handler(
            'Solver', rpc_method_handlers)
    server.add_generic_rpc_handlers((generic_handler,))


 # This class is part of an EXPERIMENTAL API.
class Solver(object):
    """Missing associated documentation comment in .proto file."""

    @staticmethod
    def Solve(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/Solver/Solve',
            instances__pb2.Cnf.SerializeToString,
            instances__pb2.Interpretation.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)
