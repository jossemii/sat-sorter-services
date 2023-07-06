from node_driver.gateway.protos import gateway_pb2, celaut_pb2
from src.envs import DEV_MODE, DEV_ENVS


def get_grpc_uri(instance: celaut_pb2.Instance) -> celaut_pb2.Instance.Uri:
    for slot in instance.api.slot:
        # if 'grpc' in slot.transport_protocol.metadata.tag and 'http2' in slot.transport_protocol.metadata.tag:
        # If the protobuf lib. supported map for this message it could be O(n).
        for uri_slot in instance.uri_slot:
            if uri_slot.internal_port == slot.port:
                return uri_slot.uri[0]
    raise Exception('Grpc over Http/2 not supported on this service ' + str(instance))


def read_file(filename) -> bytes:
    def generator(filename):
        with open(filename, 'rb') as entry:
            for chunk in iter(lambda: entry.read(1024 * 1024), b''):
                yield chunk

    return b''.join([b for b in generator(filename)])


def to_gas_amount(gas_amount: int) -> gateway_pb2.GasAmount:
    return gateway_pb2.GasAmount(n=str(gas_amount))


def from_gas_amount(gas_amount: gateway_pb2.GasAmount) -> int:
    return int(gas_amount.n)


# Should be on the Framework.
def get_client_id() -> str:
    return DEV_ENVS['CLIENT_ID'] if DEV_MODE else ''
