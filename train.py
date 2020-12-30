import threading

import requests, json
from start import DIR
from start import SAVE_TRAIN_DATA as REFRESH
from singleton import Singleton
import _solve


class Session(metaclass=Singleton):

    def __init__(self):
        self.thread = None
        self.random_service_instance = None
        self.solvers = json.load(open(DIR + 'solvers.json', 'r'))
        self.working = True
        self._solver = _solve.Session()

    def stop(self):
        self.random_service_instance.stop()
        self.working = False
        self.thread.join()

    def load_solver(self, solver):
        self.solvers.update({solver: {}})

    def init_random_cnf_service(self):
        self.random_service_instance = _solve.get_image_uri('07a9852b10c5bbc9c55180d43d70561854f6a8f5fc8a28483bf893cac0871e0b')

    def random_cnf(self):
        while True:
            try:
                print('OBTENIENDO RANDON CNF')
                response = requests.get(
                    'http://' + self.random_service_instance.uri + '/',
                    timeout=self._solver.avr_time
                )
                print('RESPUESTA DEL CNF --> ', response, response.text)
                if response and response.status_code == 200 and 'cnf' in response.json():
                    return response.json().get('cnf')
            except requests.exceptions.ConnectionError:
                pass
            except (TimeoutError, requests.exceptions.ReadTimeout, requests.HTTPError):
                print('VAMOS A CAMBIAR EL SERVICIO DE OBTENCION DE CNFs RANDOM')
                self.random_service_instance.stop()
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

    def start(self):
        self.thread = threading.Thread(target=self.init, name='Trainer', daemon=True)
        self.thread.start()

    def init(self):
        refresh = 0
        timeout = 30
        print('INICIANDO SERVICIO DE RANDOM CNF')
        self.init_random_cnf_service()
        print('hecho.')
        while self.working:
            if refresh < REFRESH:
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
                        interpretation, time = self._solver.cnf(cnf=cnf, solver=solver, timeout=timeout)
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
