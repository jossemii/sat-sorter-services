from threading import get_ident, Thread, Lock, Event

import grpc, json

import api_pb2, api_pb2_grpc
from start import DIR, TRAIN_SOLVERS_TIMEOUT, LOGGER, CONNECTION_ERRORS, START_AVR_TIMEOUT
from start import SAVE_TRAIN_DATA as REFRESH, RANDOM_SERVICE
from singleton import Singleton
import _solve


class Session(metaclass=Singleton):

    def __init__(self):
        self.thread = None
        self.random_service_instance = None
        self.solvers = json.load(open(DIR + 'solvers.json', 'r'))
        self.solvers_lock = Lock()
        self.exit_event = None
        self._solver = _solve.Session()

    def stop(self):
        if self.exit_event and self.thread:
            self.exit_event.set()
            self.thread.join()
            self.random_service_instance.stop()
            self.exit_event = None
            self.thread = None

    def load_solver(self, solver):
        self.solvers_lock.acquire()
        self.solvers.update({solver: {}})
        self.solvers_lock.release()

    def init_random_cnf_service(self):
        self.random_service_instance = _solve.get_solver_instance(RANDOM_SERVICE)
        self.random_service_instance.stub = api_pb2_grpc.RandomStub(
                grpc.insecure_channel(self.random_service_instance.uri)
            )

    def random_cnf(self):
        def new_instance():
            LOGGER('VAMOS A CAMBIAR EL SERVICIO DE OBTENCION DE CNFs RANDOM')
            self.random_service_instance.stop()
            self.init_random_cnf_service()
            LOGGER('listo. ahora vamos a probar otra vez.')

        connection_errors = 0
        while True:
            if self.random_service_instance is None: new_instance()
            try:
                LOGGER('OBTENIENDO RANDON CNF')
                LOGGER(self.random_service_instance.uri)
                return self.random_service_instance.stub.RandomCnf(
                    request=api_pb2.WhoAreYourParams(),
                    timeout=START_AVR_TIMEOUT
                )
            except (grpc.RpcError, TimeoutError) as e:
                if connection_errors < CONNECTION_ERRORS:
                    connection_errors = connection_errors + 1
                    continue
                else:
                    connection_errors = 0
                    LOGGER('  ERROR OCCURS OBTAINING THE CNF --> ' + str(e))
                    new_instance()
                    continue

    @staticmethod
    def is_good(cnf, interpretation):
        def good_clause(clause, interpretation):
            for var in clause.literal:
                for i in interpretation.variable:
                    if var == i:
                        return True
            return False

        for clause in cnf.clause:
            if not good_clause(clause, interpretation):
                return False
        return True

    def updateScore(self, cnf, solver, score):
        num_clauses, num_literals = (
            len(cnf.clause),
            0,
        )
        for clause in cnf.clause:
            for literal in clause.literal:
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
        if self.exit_event and self.thread: return None
        try:
            self.exit_event = Event()
            self.thread = Thread(target=self.init, name='Trainer')
            self.thread.start()
        except RuntimeError:
            LOGGER('Error: train thread was started and have an error.')

    def init(self):
        LOGGER('TRAINER THREAD IS ' + str(get_ident()))
        refresh = 0
        timeout = TRAIN_SOLVERS_TIMEOUT
        LOGGER('INICIANDO SERVICIO DE RANDOM CNF')
        self.init_random_cnf_service()
        LOGGER('hecho.')
        while True:
            if self.exit_event.is_set(): break
            if refresh < REFRESH:
                LOGGER('REFRESH ES MENOR')
                refresh = refresh + 1
                cnf = self.random_cnf()
                LOGGER('RESPUESTA DEL CNF --> ' + str(cnf))
                is_insat = True  # En caso en que se demuestre lo contrario.
                insats = {}  # Solvers que afirman la insatisfactibilidad junto con su respectivo tiempo.
                LOGGER('VAMOS A PROBAR LOS SOLVERS')
                self.solvers_lock.acquire()
                for solver in self.solvers:
                    LOGGER('SOVLER --> ' + str(solver))
                    interpretation, time = self._solver.cnf(cnf=cnf, solver=solver, timeout=timeout)
                    if not interpretation or not interpretation.variable:
                        insats.update({solver: time})
                    else:
                        if self.is_good(cnf, interpretation):
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
                self.solvers_lock.release()
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
                LOGGER('ACTUALIZA EL JSON')
                refresh = 0
                with open(DIR + 'solvers.json', 'w') as file:
                    json.dump(self.solvers, file)
