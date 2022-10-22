from typing import Tuple
from grpcbigbuffer import client_grpc
import grpc

from protos import api_pb2, gateway_pb2, gateway_pb2_grpc, celaut_pb2
from src.envs import ENVS, DEV_MODE
from src.utils.utils import from_gas_amount

if DEV_MODE:
    MODIFY_SYSTEM_RESOURCES_LAMBDA = lambda d: (celaut_pb2.Sysresources(
                    mem_limit=d['max']
                ), 0)
else:
    MODIFY_SYSTEM_RESOURCES_LAMBDA = lambda d: modify_resources_grpcbb(i=d)


def modify_resources_grpcbb(i: dict) -> Tuple[api_pb2.celaut__pb2.Sysresources, int]:
    output: gateway_pb2.ModifyServiceSystemResourcesOutput = next(
        client_grpc(
            method=gateway_pb2_grpc.GatewayStub(
                grpc.insecure_channel(ENVS['GATEWAY_MAIN_DIR'])
            ).ModifyServiceSystemResources,
            input=gateway_pb2.ModifyServiceSystemResourcesInput(
                min_sysreq=celaut_pb2.Sysresources(
                    mem_limit=i['min']
                ),
                max_sysreq=celaut_pb2.Sysresources(
                    mem_limit=i['max']
                ),
            ),
            partitions_message_mode_parser=True,
            indices_parser=gateway_pb2.ModifyServiceSystemResourcesOutput,
        )
    )
    return output.sysreq, from_gas_amount(output.gas)