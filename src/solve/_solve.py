from time import sleep, time as time_now
from datetime import datetime, timedelta
from threading import Thread, Lock

from dependency_manager.dependency_manager import DependencyManager
from dependency_manager.service_interface import ServiceInterface

from protos.gateway_pb2_grpcbf import StartService_input_partitions, StartService_input
from src.envs import LOGGER, SHA3_256_ID, DIR, SHA3_256, DEV_MODE
from src.utils.utils import read_file, get_grpc_uri, get_client_id
from grpcbigbuffer import Dir, client_grpc
import grpc

from protos import api_pb2, api_pb2_grpc, gateway_pb2_grpc, solvers_dataset_pb2
from protos import celaut_pb2 as celaut, gateway_pb2
from src.utils.singleton import Singleton

class Session(metaclass = Singleton):

    def __init__(self):

        LOGGER('INIT SOLVE SESSION ....')
        self.solvers = {}
        self.lock = Lock()

    def cnf(self, cnf: api_pb2.Cnf, solver_config_id: str, timeout=None):
        LOGGER(str(timeout) + 'cnf want solvers lock' + str(self.lock.locked()))

        solver_interface = self.solvers[solver_config_id]
        instance = solver_interface.get_instance()

        try:
            # Tiene en cuenta el tiempo de respuesta y deserializacion del buffer.
            start_time = time_now()
            LOGGER('    resolving cnf on ' + str(solver_config_id))
            interpretation = next(client_grpc(
                method = instance.stub.Solve,
                input = cnf,
                indices_parser = api_pb2.Interpretation,
                partitions_message_mode_parser = True,
                indices_serializer = api_pb2.Cnf,
                timeout = timeout
            ))
            time = time_now() - start_time
            LOGGER(str(time) + '    resolved cnf on ' + str(solver_config_id))
            # Si hemos o caso de que nos comunique que hay una interpretacion,
            # será satisfactible. Si no nos da interpretacion asumimos que lo identifica como insatisfactible.
            # Si ocurre un error (menos por superar el timeout) se deja la interpretación vacia (None) para,
            # tras asegurar la instancia, lanzar una excepción.
            instance.reset_timers()
            LOGGER('INTERPRETACION --> ' + str(interpretation.variable))

        except Exception as e:
            response: str = instance.compute_exception(e)
            if response == 'timeout':
                interpretation, time = api_pb2.Interpretation(), timeout
                interpretation.satisfiable = False

            elif response == 'error':
                interpretation, time = None, timeout

            else:
                LOGGER('Exception not controlled on trainer '+str(e))
                interpretation, time = None, timeout

        solver_interface.push_instance(instance)

        if interpretation:
            return interpretation, time

        else:
            raise Exception

    def add_solver(self, solver_with_config: solvers_dataset_pb2.SolverWithConfig, solver_config_id: str, solver_hash: str):
        if solver_config_id != SHA3_256(
            value = solver_with_config.SerializeToString() # This service not touch metadata, so it can use the hash for id.
        ).hex():
            LOGGER('Solver config not  valid ', solver_with_config, solver_config_id)
            raise Exception('Solver config not valid ', solver_with_config, solver_config_id)

        self.lock.acquire()
        self.solvers.update({
            solver_config_id : DependencyManager().add_service(
                service_hash = solver_hash,
                config = celaut.Configuration(
                    enviroment_variables = solver_with_config.enviroment_variables
                ),
                stub_class = api_pb2_grpc.SolverStub,
                dynamic = True
            )
        })
        self.lock.release()
        try:
            LOGGER('ADDED NEW SOLVER ' + str(solver_config_id) + ' \ndef_ids -> ' +  str(solver_with_config.meta.hashtag.hash[0].value.hex()))
        except: LOGGER('ADDED NEW SOLVER ' + str(solver_config_id))

    def get_solver_with_config(self, solver_config_id: str) -> solvers_dataset_pb2.SolverWithConfig:
        sc = self.solvers[solver_config_id].sc.get_solver_with_config()
        return solvers_dataset_pb2.SolverWithConfig(
            meta = sc.service.metadata,
            definition = sc.service.service,
            enviroment_variables = sc.config.enviroment_variables
        )