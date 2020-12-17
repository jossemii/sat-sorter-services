import requests
import json

DIR = '/satrainer/'


class Singleton(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]


class Session(metaclass=Singleton):

    def stop(self):
        self.working = False

    def load_solver(self, solver):
        self.solvers.update({solver: {}})
        self.uris.update({solver: self.get_image_uri(solver)})

    def get_image_uri(self, image):
        while True:
            print('Intenta obtener la imagen' + str(image))
            try:
                response = requests.get('http://' + self.gateway, json={'service': str(image)})
            except requests.HTTPError as e:
                print('Error al solicitar solver, ', image, e)
                pass
            if response and response.status_code == 200:
                content = response.json()
                if 'uri' in content and 'token' in content:
                    return content

    def init_random_cnf_service(self):
        random_dict = self.get_image_uri('3d67d9ded8d0abe00bdaa9a3ae83d552351afebe617f4e7b0653b5d49eb4e67a')
        self.random_uri = random_dict.get('uri')
        self.random_cnf_token = random_dict.get('token')

    def random_cnf(self):
        while True:
            try:
                print('OBTENIENDO RANDON CNF')
                response = requests.get('http://' + self.random_uri + '/', timeout=30)
                print('RESPUESTA DEL CNF --> ', response, response.text)
                if response and response.status_code == 200 and 'cnf' in response.json():
                    return response.json().get('cnf')
            except requests.exceptions.ConnectionError:
                pass
            except (TimeoutError, requests.exceptions.ReadTimeout, requests.HTTPError):
                print('VAMOS A CAMBIAR EL SERVICIO DE OBTENCION DE CNFs RANDOM')
                requests.get('http://' + self.gateway, json={'token': str(self.random_cnf_token)})
                self.init_random_cnf_service()
                print('listo. ahora vamos a probar otra vez.')

    @staticmethod
    def isGood(cnf, interpretation):
        def goodClause(clause, interpretation):
            for var in clause:
                for i in interpretation:
                    if var == i:
                        return True
            return False

        for clause in cnf:
            if goodClause(clause, interpretation) == False:
                return False
        return True

    def updateScore(self, cnf, solver, score):
        num_clauses, num_literals = (
            len(cnf),
            0,
        )
        for clause in cnf:
            for literal in clause:
                if abs(literal) > num_literals:
                    num_literals = abs(literal)
        type_of_cnf = str(num_clauses) + ':' + str(num_literals)
        if type_of_cnf in self.solvers[solver]:
            solver_cnf = self.solvers[solver][type_of_cnf]
        else:
            solver_cnf = {
                'index': 1,
                'score': 0
            }
        self.solvers[solver].update({
            str(num_clauses) + ':' + str(num_literals): {
                'index': solver_cnf['index'] + 1,
                'score': (solver_cnf['score'] * solver_cnf['index'] + score) / (solver_cnf['index'] + 1)
            }
        })

    def init(self, gateway, refresh=2):
        self.working = True
        self.refresh = int(refresh)
        self.gateway = gateway
        self.solvers = json.load(open(DIR + 'solvers.json', 'r'))
        print('OBTENIENDO IMAGENES..')
        self.uris = {solver: self.get_image_uri(solver) for solver in self.solvers}
        print('listo')
        refresh = 0
        timeout = 30
        print('INICIANDO SERVICIO DE RANDOM CNF')
        self.init_random_cnf_service()
        print('hecho.')
        while self.working:
            if refresh < self.refresh:
                print('REFRESH ES MENOR')
                refresh = refresh + 1
                cnf = self.random_cnf()
                is_insat = True  # En caso en que se demuestre lo contrario.
                insats = {}  # Solvers que afirman la insatisfactibilidad junto con su respectivo tiempo.
                print('VAMOS A PROBAR LOS SOLVERS')
                for solver in self.solvers:
                    print('SOVLER --> ', solver)
                    try:
                        # El timeout se podria calcular a partir del resto ...
                        # Tambien podria ser asincrono ...
                        print('RESPUESTA DEL SOLVER -->')
                        while True:
                            try:
                                response = requests.post(
                                    'http://' + self.uris.get(solver).get('uri') + '/',
                                    json={'cnf': cnf},
                                    timeout=timeout
                                )
                                break
                            except requests.exceptions.ConnectionError:
                                pass
                            except Exception:
                                break
                        print('INTERPRETACION --> ', response.text)

                        if response.status_code == 200:
                            interpretation = response.json().get('interpretation') or None
                        else:
                            interpretation = None

                        time = int(response.elapsed.total_seconds())
                        if interpretation == [] or interpretation is None:
                            insats.update({solver: time})
                        else:
                            if self.isGood(cnf, interpretation):
                                is_insat = False
                            else:
                                pass
                            if time == 0:
                                score = +1
                            else:
                                score = float(-1 / time)
                            self.updateScore(
                                cnf=cnf,
                                solver=solver,
                                score=score
                            )
                    except (TimeoutError, requests.exceptions.ReadTimeout):
                        print('TIME OUT NO SUPERADO.')
                        if timeout == 0:
                            score = -1
                        else:
                            score = float(-1 / timeout)
                        self.updateScore(
                            cnf=cnf,
                            solver=solver,
                            score=score
                        )
                        # Check if service is alive.
                        try:
                            requests.post(
                                'http://' + self.uris.get(solver).get('uri') + '/',
                                json={'cnf': [[1]]},
                                timeout=timeout
                            )
                        except TimeoutError:
                            print('Solicita de nuevo el servicio ' + str(solver))
                            self.uris.update({solver: self.get_image_uri(solver)})

                # Registra los solvers que afirmaron la insatisfactibilidad en caso en que ninguno
                #  haya demostrado lo contrario.
                if is_insat:
                    for solver in insats:
                        self.updateScore(
                            cnf=cnf,
                            solver=solver,
                            score=float(+1 / insats.get(solver)) if insats.get(solver) != 0 else 1
                        )
                else:
                    for solver in insats:
                        self.updateScore(
                            cnf=cnf,
                            solver=solver,
                            score=float(-1 / insats.get(solver)) if insats.get(solver) != 0 else -1
                        )
            else:
                print('ACTUALIZA EL JSON')
                refresh = 0
                with open(DIR + 'solvers.json', 'w') as file:
                    json.dump(self.solvers, file)
