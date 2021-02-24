from time import sleep, time as time_now
from datetime import datetime, timedelta
from threading import Thread, Lock, get_ident

import grpc
import requests

import api_pb2, api_pb2_grpc
from singleton import Singleton
from start import GATEWAY as GATEWAY, STOP_SOLVER_TIME_DELTA_MINUTES, LOGGER
from start import MAINTENANCE_SLEEP_TIME, SOLVER_PASS_TIMEOUT_TIMES, SOLVER_FAILED_ATTEMPTS


def get_solver_instance(image: str):
    LOGGER('\nOBTENIENDO EL SOLVER  --> ' + str(image))
    # Se debe modificar para que envie la especificacion de la imagen ...
    attemps = 0
    while True:
        if attemps < 30:
            attemps = attemps +1
        else: break
        LOGGER(' Intenta obtener la imagen' + str(image))
        try:
            response = requests.get('http://' + GATEWAY, json={'service': str(image)})
        except requests.HTTPError as e:
            LOGGER(' Error al solicitar solver, ' + str(image) + ' ' + str(e))
            pass
        if response and response.status_code == 200:
            content = response.json()
            if 'uri' in content and 'token' in content:
                instance = SolverInstance(service=image, content=content)
                instance.stub = api_pb2_grpc.SolverStub(
                    grpc.insecure_channel(instance.uri)
                )
                return instance


class SolverInstance(object):
    def __init__(self, service: str, content: dict):
        self.service = service
        self.stub = None
        self.uri = content.get('uri') or None
        self.token = content.get('token') or None
        self.creation_datetime = datetime.now()
        self.use_datetime = None
        self.pass_timeout = 0
        self.failed_attempts = 0

    def error(self):
        self.failed_attempts = self.failed_attempts + 1

    def timeout_passed(self):
        self.pass_timeout = self.pass_timeout + 1

    def stop(self):
        requests.get('http://' + GATEWAY, json={'token': str(self.token)})

    def reset_timers(self):
        self.pass_timeout = 0
        self.failed_attempts = 0

    def mark_time(self):
        self.use_datetime = datetime.now()


class Session(metaclass=Singleton):

    def __init__(self):
        LOGGER('INIT SOLVE SESSION ....')
        self.avr_time = 30
        self.solvers = {}
        self.solvers_lock = Lock()
        Thread(target=self.maintenance, name='Maintainer').start()

    def cnf(self, cnf, solver: str, timeout=None):
        LOGGER('cnf want solvers lock' + str(self.solvers_lock.locked()))
        self.solvers_lock.acquire()

        if solver not in self.solvers:
            self.add_or_update_solver(solver=solver)
        solver = self.get(solver)
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
            while True:
                LOGGER('maintainer want solvers lock' + str(self.solvers_lock.locked()))
                self.solvers_lock.acquire()
                try:
                    solver = self.solvers[list(self.solvers)[index]]
                except IndexError:
                    self.solvers_lock.release()
                    break
                LOGGER('      maintain solver --> ' + str(solver))

                # En caso de que lleve mas de dos minutos sin usarse.
                if datetime.now() - solver.use_datetime > timedelta(minutes=STOP_SOLVER_TIME_DELTA_MINUTES):
                    self.stop_solver(solver=solver)
                    self.solvers_lock.release()
                    continue
                # En caso de que tarde en dar respuesta a cnf's reales,
                #  comprueba si la instancia sigue funcionando.
                if solver.pass_timeout > SOLVER_PASS_TIMEOUT_TIMES and \
                        not self.check_if_service_is_alive(solver=solver) or \
                        solver.failed_attempts > SOLVER_FAILED_ATTEMPTS:
                    self.add_or_update_solver(solver=solver.service)

                self.solvers_lock.release()
                index = +1
                sleep(MAINTENANCE_SLEEP_TIME / index)

    def stop_solver(self, solver: SolverInstance):
        solver.stop()
        del self.solvers[solver.service]

    def check_if_service_is_alive(self, solver: SolverInstance) -> bool:
        LOGGER('Check if serlvice ' + str(solver.service) + ' is alive.')
        cnf = api_pb2.Cnf()
        clause = cnf.clause.add()
        clause.literal = 1
        try:
            api_pb2_grpc.Solver(
                grpc.insecure_channel(solver.uri)
            ).Solve(
                request=cnf,
                timeout=self.avr_time
            )
            return True
        except (TimeoutError, grpc.RpcError):
            return False

    def get(self, solver: str) -> SolverInstance or None:
        return self.solvers.get(solver) or None

    def add_or_update_solver(self, solver: str):
        if solver in self.solvers:
            self.get(solver).stop()
        self.solvers.update({solver: get_solver_instance(solver)})
