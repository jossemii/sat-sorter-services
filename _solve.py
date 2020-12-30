import requests
from singleton import Singleton
from start import GATEWAY as GATEWAY


def get_image_uri(image):
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
                return content


class Session(metaclass=Singleton):

    def __init__(self):
        self.avr_time = 30
        self.uris = {}

    def cnf(self, cnf, solver, timeout=None):
        if solver not in self.uris:
            self.uris.update({solver: get_image_uri(solver)})
        while True:
            try:
                response = requests.post(
                    'http://' + self.uris.get(solver).get('uri') + '/',
                    json={'cnf': cnf},
                    timeout=timeout or self.avr_time
                )
                break
            except requests.exceptions.ConnectionError:
                pass
            except Exception:
                break
        try:
            if response and response.status_code == 200:
                print('INTERPRETACION --> ', response.text)
                interpretation = response.json().get('interpretation') or None
                time = int(response.elapsed.total_seconds())
            else:
                interpretation, time = None, 0
        except UnboundLocalError:
            interpretation, time = None, 0

        return interpretation, time

    def stop_solvers(self):
        for solver in self.uris:
            requests.get('http://' + GATEWAY, json={'token': str(self.uris.get(solver).get('token'))})

    def check_if_service_is_alive(self, solver):
        # Check if service is alive.
        try:
            requests.post(
                'http://' + self.uris.get(solver).get('uri') + '/',
                json={'cnf': [[1]]},
                timeout=2 * self.avr_time
            )
        except TimeoutError:
            print('Solicita de nuevo el servicio ' + str(solver))
            self.uris.update({solver: get_image_uri(solver)})
