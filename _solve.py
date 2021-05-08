from time import sleep, time as time_now
from datetime import datetime, timedelta
from threading import Thread, Lock, get_ident

import grpc, hashlib

import api_pb2, api_pb2_grpc, gateway_pb2, gateway_pb2_grpc, solvers_dataset_pb2
from singleton import Singleton
from start import LOGGER

# -- HASH FUNCTIONS --
SHAKE_256 = lambda value: "" if value is None else 'shake-256:0x'+hashlib.shake_256(value).hexdigest(32)
SHA3_256 = lambda value: "" if value is None else 'sha3-256:0x'+hashlib.sha3_256(value).hexdigest()

HASH_LIST = ['SHAKE_256', 'SHA3_256']

class SolverInstance(object):
    def __init__(self, solver_with_config: solvers_dataset_pb2.SolverWithConfig):
        self.service_def = gateway_pb2.ipss__pb2.Service()
        self.service_def.CopyFrom(solver_with_config.definition)
        
        # Configuration.
        self.config = gateway_pb2.ipss__pb2.Configuration()
        self.config.enviroment_variables.update(solver_with_config.enviroment_variables)
        self.config.spec_slot.append( solver_with_config.definition.api[0].port ) # solo tomamos el primer slot. ¡suponemos que se encuentra alli toda la api!

        self.stub = None
        self.token = gateway_pb2.Token()
        self.creation_datetime = datetime.now()
        self.use_datetime = datetime.now()
        self.pass_timeout = 0
        self.failed_attempts = 0

    def service_extended(self):
        config = True
        transport = gateway_pb2.ServiceTransport()
        for hash in self.service_def.hash:
            transport.hash = hash
            if config: # Solo hace falta enviar la configuracion en el primer paquete.
                transport.config.CopyFrom(self.config)
                config = False
            yield transport
        transport.ClearField('hash')
        if config: transport.config.CopyFrom(self.config)
        transport.service.CopyFrom(self.service_def)
        yield transport

    def update_solver_stub(self, instance: gateway_pb2.ipss__pb2.Instance):
        uri = instance.instance.uri_slot[0].uri[0]
        self.stub = api_pb2_grpc.SolverStub(
            grpc.insecure_channel(
                uri.ip+':'+str(uri.port)
            )
        )
        self.token.CopyFrom(instance.token)

    def error(self):
        self.failed_attempts = self.failed_attempts + 1

    def timeout_passed(self):
        self.pass_timeout = self.pass_timeout + 1

    def reset_timers(self):
        self.pass_timeout = 0
        self.failed_attempts = 0

    def mark_time(self):
        self.use_datetime = datetime.now()

    def check_if_service_is_alive(self) -> bool:
        LOGGER('Check if serlvice ' + str(self.multihash[0]) + ' is alive.')
        cnf = api_pb2.Cnf()
        clause = cnf.clause.add()
        clause.literal = 1
        try:
            self.stub.Solve(
                request=cnf,
                timeout=self.avr_time
            )
            return True
        except (TimeoutError, grpc.RpcError):
            return False


class Session(metaclass=Singleton):

    def __init__(self, ENVS):

        # set used envs on variables.
        self.GATEWAY_MAIN_DIR = ENVS['GATEWAY_MAIN_DIR']
        self.MAINTENANCE_SLEEP_TIME = ENVS['MAINTENANCE_SLEEP_TIME']
        self.SOLVER_PASS_TIMEOUT_TIMES = ENVS['SOLVER_PASS_TIMEOUT_TIMES']
        self.STOP_SOLVER_TIME_DELTA_MINUTES = ENVS['STOP_SOLVER_TIME_DELTA_MINUTES']
        self.SOLVER_FAILED_ATTEMPTS = ENVS['SOLVER_FAILED_ATTEMPTS']

        LOGGER('INIT SOLVE SESSION ....')
        self.avr_time = 30
        self.solvers = {}
        self.gateway_stub = gateway_pb2_grpc.GatewayStub(grpc.insecure_channel(self.GATEWAY_MAIN_DIR))
        self.solvers_lock = Lock()
        Thread(target=self.maintenance, name='Maintainer').start()

    def update_solver_stub(self, solver_config_id: str):
        solver_instance = self.solvers[solver_config_id]
        try:
            solver_instance.update_solver_stub(
                self.gateway_stub.StartService(solver_instance.service_extended())
            )
        except grpc.RpcError as e:
            LOGGER('GRPC ERROR.'+ str(e))

    def cnf(self, cnf, solver_config_id: str, timeout=None):
        LOGGER('cnf want solvers lock' + str(self.solvers_lock.locked()))
        self.solvers_lock.acquire()

        if solver_config_id not in self.solvers: raise Exception
        solver = self.solvers[solver_config_id]
        solver.mark_time()
        try:
            # Tiene en cuenta el tiempo de respuesta y deserializacion del buffer.
            start_time = time_now()
            interpretation = solver.stub.Solve(
                request=cnf,
                timeout=self.avr_time
            )
            time = time_now() - start_time
            # Si hemos obtenido una respuesta, en caso de que nos comunique que hay una interpretacion,
            #  si no nos da interpretacion asumimos que lo identifica como insatisfactible.
            solver.reset_timers()
            LOGGER('INTERPRETACION --> ' + str(interpretation.variable))
        except TimeoutError:
            LOGGER('TIME OUT NO SUPERADO.')
            solver.timeout_passed()
            interpretation, time = None, timeout
        except grpc.RpcError as e:
            LOGGER('GRPC ERROR.'+ str(e))
            solver.error()
            interpretation, time = None, timeout
        self.solvers_lock.release()
        return interpretation, time

    def maintenance(self):
        while True:
            LOGGER('MAINTEANCE THREAD IS ' + str(get_ident()))
            sleep(self.MAINTENANCE_SLEEP_TIME)

            
            index = 0
            while True: # Si hacemos for solver in solvers habría que bloquear el bucle entero.
                LOGGER('maintainer want solvers lock' + str(self.solvers_lock.locked()))
                self.solvers_lock.acquire()
                try:
                    solver_id = list(self.solvers)[index]
                    solver_instance = self.solvers[solver_id]
                except IndexError:
                    self.solvers_lock.release()
                    break
                LOGGER('      maintain solver --> ' + str(solver_instance))

                # En caso de que lleve mas de dos minutos sin usarse.
                if datetime.now() - solver_instance.use_datetime > timedelta(minutes=self.STOP_SOLVER_TIME_DELTA_MINUTES):
                    self.stop_solver(id=solver_id)
                    self.solvers_lock.release()
                    continue
                # En caso de que tarde en dar respuesta a cnf's reales,
                #  comprueba si la instancia sigue funcionando.
                if solver_instance.pass_timeout > self.SOLVER_PASS_TIMEOUT_TIMES and \
                        not solver_instance.check_if_service_is_alive() or \
                        solver_instance.failed_attempts > self.SOLVER_FAILED_ATTEMPTS:
                    self.update_solver_stub(solver_config_id=solver_id)

                self.solvers_lock.release()
                index = +1
                sleep(self.MAINTENANCE_SLEEP_TIME / index)

    def stop_solver(self, id: str):
        try:
            self.gateway_stub.StopService(
                self.solvers[id].token
            )
        except grpc.RpcError as e:
            LOGGER('GRPC ERROR.'+ str(e))
        del self.solvers[id]

    def add_solver(self, solver_with_config: solvers_dataset_pb2.SolverWithConfig, solver_config_id: str):
        self.solvers.update({
            solver_config_id: SolverInstance(
                    solver_with_config=solver_with_config
                )
            })
        self.update_solver_stub(solver_config_id=solver_config_id)
