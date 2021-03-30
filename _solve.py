from time import sleep, time as time_now
from datetime import datetime, timedelta
from threading import Thread, Lock, get_ident

import grpc, hashlib

import api_pb2, api_pb2_grpc, gateway_pb2, gateway_pb2_grpc, solvers_dataset_pb2
from singleton import Singleton
from start import GATEWAY_MAIN_DIR as GATEWAY_MAIN_DIR, STOP_SOLVER_TIME_DELTA_MINUTES, LOGGER
from start import MAINTENANCE_SLEEP_TIME, SOLVER_PASS_TIMEOUT_TIMES, SOLVER_FAILED_ATTEMPTS


# -- HASH FUNCTIONS --
SHAKE_256 = lambda value: "" if value is None else hashlib.shake_256(value).hexdigest(32)
SHA3_256 = lambda value: "" if value is None else hashlib.sha3_256(value).hexdigest(32)

# ALERT: Its not async.
SHAKE_STREAM = lambda value: "" if value is None else hashlib.shake_256(value).hexdigest(99999999)

HASH_LIST = ['SHAKE256', 'SHA3_256']

class SolverInstance(object):
    def __init__(self, solver_with_config: solvers_dataset_pb2.SolverWithConfig):
        self.service_def = gateway_pb2.ipss__pb2.Service()
        self.service_def.CopyFrom(solver_with_config.definition)
        self.config = gateway_pb2.ipss__pb2.Configuration()
        self.config.enviroment_variables = solver_with_config.enviroment_variables
        slot = gateway_pb2.ipss__pb2.Configuration.SlotSpec()
        slot.port = solver_with_config.definition.api[0].port # solo tomamos el primer slot. ¡suponemos que se encuentra alli toda la api!
        slot.transport_protocol.tag.append('http2','grpc')
        self.config.slot.append(slot)

        self.stub = None
        self.token = None
        self.creation_datetime = datetime.now()
        self.use_datetime = None
        self.pass_timeout = 0
        self.failed_attempts = 0
        # Calculate service hashes.
        self.multihash = {}
        for hash in HASH_LIST:
            self.multihash.update({
                hash: eval(hash)(
                    solver_with_config.definition.SerializeToString()
                )
            })

    def service_extended(self):
        config = True
        se = gateway_pb2.ServiceExtended()
        for hash in self.multihash:
            h = gateway_pb2.ipss__pb2.Hash()
            h.hash = self.multihash[hash]
            h.tag.append(hash)
            se.hash.CopyFrom(hash)
            if config: # Solo hace falta enviar la configuracion en el primer paquete.
                se.config.CopyFrom(self.config)
                config = False
            yield se
        se.ClearField('hash')
        se.service.CopyFrom(self.service_def)
        yield se

    def update_solver_stub(self, instance: gateway_pb2.ipss__pb2.Instance):
        uri = instance.instance.uri_slot[
            self.config.slot[0].port
        ]
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
        LOGGER('Check if serlvice ' + str(self.service_def) + ' is alive.')
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

    def __init__(self):
        LOGGER('INIT SOLVE SESSION ....')
        self.avr_time = 30
        self.solvers = {}
        self.gateway_stub = gateway_pb2_grpc.GatewayStub(grpc.insecure_channel(GATEWAY_MAIN_DIR))
        self.solvers_lock = Lock()
        Thread(target=self.maintenance, name='Maintainer').start()

    def update_solver_stub(self, solver_config_id: str):
        solver_instance = self.solvers[solver_config_id]
        try:
            solver_instance.update_solver_stub(
                self.gateway_stub.StartServiceWithExtended(solver_instance.service_extended())
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
            sleep(MAINTENANCE_SLEEP_TIME)

            
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
                if datetime.now() - solver_instance.use_datetime > timedelta(minutes=STOP_SOLVER_TIME_DELTA_MINUTES):
                    self.stop_solver(solver=solver_id)
                    self.solvers_lock.release()
                    continue
                # En caso de que tarde en dar respuesta a cnf's reales,
                #  comprueba si la instancia sigue funcionando.
                if solver_instance.pass_timeout > SOLVER_PASS_TIMEOUT_TIMES and \
                        not solver_instance.check_if_service_is_alive() or \
                        solver_instance.failed_attempts > SOLVER_FAILED_ATTEMPTS:
                    self.update_solver_stub(solver_config_id=solver_id)

                self.solvers_lock.release()
                index = +1
                sleep(MAINTENANCE_SLEEP_TIME / index)

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
                    solver_with_config=solver_config_id
                )
            })
        self.update_solver_stub(solver_config_id=solver_config_id)