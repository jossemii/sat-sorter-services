from threading import get_ident, Thread, Lock, Event

import grpc, json

import api_pb2, api_pb2_grpc, performance_data_pb2, hashlib, gateway_pb2, gateway_pb2_grpc
from start import DIR, TRAIN_SOLVERS_TIMEOUT, LOGGER, CONNECTION_ERRORS, START_AVR_TIMEOUT
from start import SAVE_TRAIN_DATA as REFRESH, RANDOM_SERVICE, GATEWAY_MAIN_DIR
from singleton import Singleton
import _solve


class Session(metaclass=Singleton):

    def __init__(self):
        self.thread = None
        self.gateway_stub = gateway_pb2_grpc.GatewayStub(grpc.insecure_channel(GATEWAY_MAIN_DIR))
        with open('random.service', 'rb') as file:
            self.random_def = gateway_pb2.ipss__pb2.Service()
            self.random_def.ParseFromString(file.read())
        self.random_stub = None
        self.random_token = None
        self.solvers_dataset = performance_data_pb2.SolversPerformance()
        self.solvers = []
        self.solvers_lock = Lock()
        self.exit_event = None
        self._solver = _solve.Session()

    def stop(self):
        if self.exit_event and self.thread:
            self.exit_event.set()
            self.thread.join()
            try:
                self.gateway_stub.StopService(self.random_token)
            except grpc.RpcError as e:
                LOGGER('GRPC ERROR.'+ str(e))
            self.exit_event = None
            self.thread = None

    def load_solver(self, solver: performance_data_pb2.ipss__pb2.Service):
        hash = hashlib.sha256(solver.SerializeToString())
        if hash not in self.solvers:
            self.solvers.append(hash)
            self.solvers_lock.acquire()
            p = performance_data_pb2.Performance()
            p.solver.definition.CopyFrom(solver)
            # p.solver.enviroment_variables (Usamos las variables de entorno por defecto).
            p.hash = hashlib.sha256(p.solver.SerializeToString())
            self.solvers_dataset.data.append(p)
            self._solver.add_solver(solver_with_config=p.solver, solver_config_id=p.hash)
            self.solvers_lock.release()

    def init_random_cnf_service(self):
        # Pasamos directamente la especificacion del servicio sin configuracion.
        service_extended = gateway_pb2.ServiceExtended()
        service_extended.service.CopyFrom(self.random_def)
        try:
            instance = self.gateway_stub.StartService(service_extended)
        except grpc.RpcError as e:
            LOGGER('GRPC ERROR.'+ str(e))
        uri = instance.instance.uri_slot[
            self.random_def.api[0].port
        ]
        self.random_stub = api_pb2_grpc.RandomStub(
                grpc.insecure_channel(
                    uri.ip+':'+str(uri.port)
                )
            )
        self.random_token.CopyFrom(instance.token)

    def random_cnf(self):
        def new_instance():
            LOGGER('VAMOS A CAMBIAR EL SERVICIO DE OBTENCION DE CNFs RANDOM')
            try:
                self.gateway_stub.StopService(self.random_token)
            except grpc.RpcError as e:
                LOGGER('GRPC ERROR.'+ str(e))
            self.init_random_cnf_service()
            LOGGER('listo. ahora vamos a probar otra vez.')

        connection_errors = 0
        while True:
            try:
                LOGGER('OBTENIENDO RANDON CNF')
                return self.random_stub.RandomCnf(
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

    def updateScore(self, cnf, solver: performance_data_pb2.Performance, score):
        num_clauses, num_literals = (
            len(cnf.clause),
            0,
        )
        for clause in cnf.clause:
            for literal in clause.literal:
                if abs(literal) > num_literals:
                    num_literals = abs(literal)
        type_of_cnf = str(num_clauses) + ':' + str(num_literals)
        if type_of_cnf in solver.data:
            solver.data[type_of_cnf].index = 1
            solver.data[type_of_cnf].score = 0
        solver.data[type_of_cnf].score = (solver.data[type_of_cnf].score * solver.data[type_of_cnf].index + score) / (solver.data[type_of_cnf].index + 1)
        solver.data[type_of_cnf].index = solver.data[type_of_cnf].index + 1

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
                for solver in self.solvers_dataset:
                    LOGGER('SOVLER --> ' + str(solver.hash))
                    interpretation, time = self._solver.cnf(cnf=cnf, solver_config_id=solver.hash, timeout=timeout)
                    if not interpretation or not interpretation.variable:
                        insats.update({solver.hash: time})
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
                LOGGER('ACTUALIZA EL DATASET')
                refresh = 0
                with open(DIR + 'solvers_dataset.bin', 'wb') as file:
                    file.write(self.solvers_dataset.SerializeToString())
