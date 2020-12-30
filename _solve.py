from time import sleep
from datetime import datetime, timedelta
import requests
from singleton import Singleton
from start import GATEWAY as GATEWAY


def get_image_uri(image: str):
    print('Obteniendo solver --> ', str(image))
    while True:
        print('Intenta obtener la imagen' + str(image))
        try:
            response = requests.get('http://' + GATEWAY, json={'service': str(image)})
        except requests.HTTPError as e:
            print('Error al solicitar solver, ', image, e)
            pass
        if response and response.status_code == 200:
            content = response.json()
            if 'uri' in content and 'token' in content:
                return SolverInstance(service=image, content=content)


class SolverInstance(object):
    def __init__(self, service: str, content: dict):
        self.service = service
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
        print('INIT SOLVE SESSION ....')
        self.avr_time = 30
        self.solvers = {}
        self.maintenance()  # open other threath

    def cnf(self, cnf, solver: str, timeout=None):
        if solver not in self.solvers:
            self.add_or_update_solver(solver=solver)
        solver = self.get(solver)
        solver.mark_time()
        while True:
            try:
                response = requests.post(
                    'http://' + solver.uri + '/',
                    json={'cnf': cnf},
                    timeout=timeout or self.avr_time
                )
                break
            except TimeoutError:
                solver.timeout_passed()
            except requests.exceptions.ConnectionError or BaseException or requests.HTTPError:
                solver.error()
        if response and response.status_code == 200:
            solver.reset_timers()
            print('INTERPRETACION --> ', response.text)
            interpretation = response.json().get('interpretation') or None
            time = int(response.elapsed.total_seconds())
            return interpretation, time
        else:
            return None, 0

    def maintenance(self):
        while True:
            sleep(100)
            for solver in self.solvers.values():
                # En caso de que lleve mas de dos minutos sin usarse.
                if datetime.now() - solver.use_datetime > timedelta(minutes=2):
                    self.stop_solver(solver=solver.service)
                    continue
                # En caso de que tarde en dar respuesta a cnf's reales,
                #  comprueba si la instancia sigue funcionando.
                if solver.pass_timeout > 5 and not self.check_if_service_is_alive(solver=solver) \
                        or solver.failed_attempts > 5:
                    self.add_or_update_solver(solver=solver.service)

    def stop_solver(self, solver: SolverInstance):
        solver.stop()
        del self.solvers[solver.service]

    def check_if_service_is_alive(self, solver: SolverInstance) -> bool:
        print('Check if serlvice ', solver.service, ' is alive.')
        try:
            requests.post(
                'http://' + solver.uri + '/',
                json={'cnf': [[1]]},
                timeout=2 * self.avr_time
            )
            return requests.status_codes == 200
        except TimeoutError or requests.exceptions.ConnectionError or BaseException or requests.HTTPError:
            return False

    def get(self, solver: str) -> SolverInstance or None:
        return self.solvers.get(solver) or None

    def add_or_update_solver(self, solver: str):
        if solver in self.solvers:
            self.get(solver).stop()
        self.solvers.update({solver: get_image_uri(solver)})
