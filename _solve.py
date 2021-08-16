from time import sleep, time as time_now
from datetime import datetime, timedelta
from threading import Thread, Lock, get_ident

from google.protobuf.descriptor import Error

import grpc, hashlib

import api_pb2, api_pb2_grpc, gateway_pb2, gateway_pb2_grpc, solvers_dataset_pb2
from singleton import Singleton
from start import LOGGER, get_grpc_uri

# Si se toma una instancia, se debe de asegurar que, o bien se agrega a su cola
#  correspondiente, o bien se para. No asegurar esto ocasiona un bug importante
#  ya que las instancias quedarían zombies en la red hasta que el clasificador
#  fuera eliminado.

class SolverInstance(object):
    def __init__(self, stub, token):
        self.stub = stub
        self.token = token
        self.creation_datetime = datetime.now()
        self.use_datetime = datetime.now()
        self.pass_timeout = 0
        self.failed_attempts = 0

    def error(self):
        self.failed_attempts = self.failed_attempts + 1

    def is_zombie(self,
                  SOLVER_PASS_TIMEOUT_TIMES,
                  TRAIN_SOLVERS_TIMEOUT,
                  SOLVER_FAILED_ATTEMPTS
                  ) -> bool:
        # En caso de que tarde en dar respuesta a cnf's reales,
        #  comprueba si la instancia sigue funcionando.
        return self.pass_timeout > SOLVER_PASS_TIMEOUT_TIMES and \
               not self.check_if_is_alive(timeout=TRAIN_SOLVERS_TIMEOUT) \
               or self.failed_attempts > SOLVER_FAILED_ATTEMPTS

    def timeout_passed(self):
        self.pass_timeout = self.pass_timeout + 1

    def reset_timers(self):
        self.pass_timeout = 0
        self.failed_attempts = 0

    def mark_time(self):
        self.use_datetime = datetime.now()

    def check_if_is_alive(self, timeout) -> bool:
        LOGGER('Check if instance ' + str(self.token) + ' is alive.')
        cnf = api_pb2.Cnf()
        clause = api_pb2.Clause()
        clause.literal.append(1)
        cnf.clause.append(clause)
        try:
            self.stub.Solve(
                request=cnf,
                timeout=timeout
            )
            return True
        except (TimeoutError, grpc.RpcError):
            return False

    def stop(self, gateway_stub):
        LOGGER('Stops this instance with token ' + str(self.token))
        while True:
            try:
                gateway_stub.StopService(
                    gateway_pb2.TokenMessage(
                        token = self.token
                    )
                )
                break
            except grpc.RpcError as e:
                LOGGER('GRPC ERROR STOPPING SOLVER ' + str(e))
                sleep(1)


class SolverConfig(object):
    def __init__(self, solver_with_config: solvers_dataset_pb2.SolverWithConfig):
        self.service_def = gateway_pb2.ipss__pb2.Service()
        self.service_def.CopyFrom(solver_with_config.definition)

        # Configuration.
        self.config = gateway_pb2.ipss__pb2.Configuration()
        self.config.enviroment_variables.update(solver_with_config.enviroment_variables)

        self.instances = []  # se da uso de una pila para que el 'maintainer' detecte las instancias que quedan en desuso,
        #  ya que quedarán estancadas al final de la pila.

    def service_extended(self):
        config = True
        transport = gateway_pb2.ServiceTransport()
        for hash in self.service_def.hashtag.hash:
            transport.hash.CopyFrom(hash)
            if config:  # Solo hace falta enviar la configuracion en el primer paquete.
                transport.config.CopyFrom(self.config)
                config = False
            yield transport
        transport.ClearField('hash')
        if config: transport.config.CopyFrom(self.config)
        transport.service.CopyFrom(self.service_def)
        yield transport

    def launch_instance(self, gateway_stub) -> SolverInstance:
        LOGGER('    launching new instance for solver ' + str(self.service_def.hashtag.hash[0].value.hex()))
        while True:
            try:
                instance = gateway_stub.StartService(self.service_extended()) # Sin timeout, por si tiene que construirlo.
                break
            except grpc.RpcError as e:
                LOGGER('GRPC ERROR.' + str(e))

        try:
            uri = get_grpc_uri(instance.instance)
        except Exception as e:
            LOGGER(str(e))
            raise e
        LOGGER('THE URI FOR THE SOLVER ' + str(self.service_def.hashtag.hash[0].value.hex()) + ' is--> ' + str(uri))

        return SolverInstance(
            stub = api_pb2_grpc.SolverStub(
                    grpc.insecure_channel(
                        uri.ip + ':' + str(uri.port)
                    )
            ),
            token = instance.token
        )

    def add_instance(self, instance: SolverInstance, deep=False):
        LOGGER('Add instance ' + str(instance))
        self.instances.append(instance) if not deep else self.instances.insert(0, instance)

    def get_instance(self, deep=False) -> SolverInstance:
        LOGGER('Get an instance. deep ' + str(deep))
        try:
            return self.instances.pop() if not deep else self.instances.pop(0)
        except IndexError:
            LOGGER('    list empty --> ' + str(self.instances))
            raise IndexError


class Session(metaclass=Singleton):

    def __init__(self, ENVS: dict):

        # set used envs on variables.
        self.GATEWAY_MAIN_DIR = ENVS['GATEWAY_MAIN_DIR']
        self.MAINTENANCE_SLEEP_TIME = ENVS['MAINTENANCE_SLEEP_TIME']
        self.SOLVER_PASS_TIMEOUT_TIMES = ENVS['SOLVER_PASS_TIMEOUT_TIMES']
        self.SOLVER_FAILED_ATTEMPTS = ENVS['SOLVER_FAILED_ATTEMPTS']
        self.TRAIN_SOLVERS_TIMEOUT = ENVS['TRAIN_SOLVERS_TIMEOUT']
        self.MAX_DISUSE_TIME_FACTOR = ENVS['MAX_DISUSE_TIME_FACTOR']

        LOGGER('INIT SOLVE SESSION ....')
        self.solvers = {}
        self.gateway_stub = gateway_pb2_grpc.GatewayStub(grpc.insecure_channel(self.GATEWAY_MAIN_DIR))
        self.lock = Lock()
        Thread(target=self.maintenance, name='Maintainer').start()

    def cnf(self, cnf, solver_config_id: str, timeout=None, solver_with_config=None):
        LOGGER(str(timeout) + 'cnf want solvers lock' + str(self.lock.locked()))
        self.lock.acquire()

        if solver_with_config and solver_config_id not in self.solvers:
            self.lock.release()
            LOGGER('ERROR SOLVING CNF, SOLVER_CONFIG_ID NOT IN _Solve.solvers list.' \
                   + str(self.solvers.keys()) + ' ' + str(solver_config_id))

            self.add_solver(
                solver_config_id=solver_config_id,
                solver_with_config=solver_with_config
            )
            return Error

        solver_config = self.solvers[solver_config_id]
        try:
            instance = solver_config.get_instance()  # use the list like a stack.
            self.lock.release()
        except IndexError:
            # Si no hay ninguna instancia disponible, deja el lock y solicita al nodo una nueva.
            self.lock.release()
            instance = solver_config.launch_instance(self.gateway_stub)

        instance.mark_time()
        try:
            # Tiene en cuenta el tiempo de respuesta y deserializacion del buffer.
            start_time = time_now()
            LOGGER('    resolving cnf on ' + str(solver_config_id))
            interpretation = instance.stub.Solve(
                request=cnf,
                timeout=timeout
            )
            time = time_now() - start_time
            LOGGER(str(time) + '    resolved cnf on ' + str(solver_config_id))
            # Si hemos obtenido una respuesta, en caso de que nos comunique que hay una interpretacion,
            # será satisfactible. Si no nos da interpretacion asumimos que lo identifica como insatisfactible.
            # Si ocurre un error (menos por superar el timeout) se deja la interpretación vacia (None) para,
            # tras asegurar la instancia, lanzar una excepción.
            instance.reset_timers()
            LOGGER('INTERPRETACION --> ' + str(interpretation.variable))
        except grpc.RpcError as e:
            if int(e.code().value[0]) == 4:  # https://github.com/avinassh/grpc-errors/blob/master/python/client.py
                LOGGER('TIME OUT NO SUPERADO.')
                instance.timeout_passed()
                interpretation, time = api_pb2.Interpretation(), timeout
                interpretation.satisfiable = False
            else:
                LOGGER('GRPC ERROR.' + str(e))
                instance.error()
                interpretation, time = None, timeout
        except Exception as e:
            LOGGER('ERROR ON CNF ' + str(e))
            instance.error()
            interpretation, time = None, timeout

        # Si la instancia se encuentra en estado zombie
        # la detiene, en caso contrario la introduce
        #  de nuevo en su cola correspondiente.
        if instance.is_zombie(
                self.SOLVER_PASS_TIMEOUT_TIMES,
                self.TRAIN_SOLVERS_TIMEOUT,
                self.SOLVER_FAILED_ATTEMPTS
        ):
            instance.stop(self.gateway_stub)
        else:
            self.lock.acquire()
            solver_config.add_instance(instance)
            self.lock.release()

        if interpretation:
            return interpretation, time
        else:
            raise Exception

    def maintenance(self):
        LOGGER('MAINTEANCE THREAD IS ' + str(get_ident()))
        while True:
            sleep(self.MAINTENANCE_SLEEP_TIME)
            index = 0
            while True:  # Si hacemos for solver in solvers habría que bloquear el bucle entero.
                sleep(self.MAINTENANCE_SLEEP_TIME)
                LOGGER('maintainer want solvers lock' + str(self.lock.locked()))
                self.lock.acquire()
                # Toma aqui el máximo tiempo de desuso para aprovechar el uso del lock.
                max_disuse_time = max(
                    len(self.solvers) * self.TRAIN_SOLVERS_TIMEOUT,
                    self.MAINTENANCE_SLEEP_TIME
                )
                try:
                    solver_config = self.solvers[
                        list(self.solvers)[index]
                    ]
                    try:
                        instance = solver_config.get_instance(deep=True)

                        # Toma aqui el máximo tiempo de desuso para aprovechar el lock.
                        # Si salta una excepción la variable no vuelve a ser usada.
                        max_disuse_time = len(self.solvers) * self.TRAIN_SOLVERS_TIMEOUT * self.MAX_DISUSE_TIME_FACTOR
                    except IndexError:
                        # No hay instancias disponibles en esta cola.
                        self.lock.release()
                        continue
                except IndexError:
                    # Se han recorrido todos los solvers.
                    self.lock.release()
                    break
                except Exception as e:
                    LOGGER('ERROR on maintainer, ' + str(e))
                    self.lock.release()
                    break
                self.lock.release()

                LOGGER('      maintain solver instance --> ' + str(instance))
                # En caso de que lleve mas de demasiado tiempo sin usarse.
                # o se encuentre en estado 'zombie'
                if datetime.now() - instance.use_datetime > timedelta(
                        minutes=max_disuse_time) \
                        or instance.is_zombie(
                    self.SOLVER_PASS_TIMEOUT_TIMES,
                    self.TRAIN_SOLVERS_TIMEOUT,
                    self.SOLVER_FAILED_ATTEMPTS
                ):
                    instance.stop(self.gateway_stub)
                # En caso contrario añade de nuevo la instancia a su respectiva cola.
                else:
                    self.lock.acquire()
                    solver_config.add_instance(instance, deep=True)
                    self.lock.release()

    def add_solver(self, solver_with_config: solvers_dataset_pb2.SolverWithConfig, solver_config_id: str):
        LOGGER('ADDED NEW SOLVER ' + str(solver_config_id))
        self.lock.acquire()
        self.solvers.update({
            solver_config_id: SolverConfig(
                solver_with_config = solver_with_config
            )
        })
        self.lock.release()
