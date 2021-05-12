from threading import get_ident, Thread, Lock, Event
import grpc, hashlib
from time import sleep
import api_pb2, api_pb2_grpc, solvers_dataset_pb2, gateway_pb2, gateway_pb2_grpc
from singleton import Singleton
import _solve
from start import LOGGER, DIR


class Session(metaclass=Singleton):

    def __init__(self, ENVS):

        # set used envs on variables.
        self.GATEWAY_MAIN_DIR = ENVS['GATEWAY_MAIN_DIR']
        self.START_AVR_TIMEOUT = ENVS['START_AVR_TIMEOUT']
        self.CONNECTION_ERRORS = ENVS['CONNECTION_ERRORS']
        self.TRAIN_SOLVERS_TIMEOUT = ENVS['TRAIN_SOLVERS_TIMEOUT']
        self.REFRESH = ENVS['SAVE_TRAIN_DATA']

        self.thread = None
        self.gateway_stub = gateway_pb2_grpc.GatewayStub(grpc.insecure_channel(self.GATEWAY_MAIN_DIR))
        with open(DIR+'random.service', 'rb') as file:
            self.random_def = gateway_pb2.ipss__pb2.Service()
            self.random_def.ParseFromString(file.read())

        self.random_stub = None
        self.random_token = gateway_pb2.Token()
        self.solvers_dataset = solvers_dataset_pb2.DataSet()
        self.solvers = []
        self.solvers_lock = Lock()
        self.exit_event = None
        self._solver = _solve.Session(ENVS=ENVS)

        # Random CNF Service.
        self.random_config = gateway_pb2.ipss__pb2.Configuration()
        self.random_config.spec_slot.append(
            self.random_def.api[0].port
        )

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

    def load_solver(self, solver: solvers_dataset_pb2.ipss__pb2.Service):
        # Se puede cargar un solver sin estar completo, 
        #  pero debe de contener si o si la sha3-256
        #  ya que el servicio no la calculará (ni comprobará).

        for h in solver.hash:
            if h.split(':')[0] == 'sha3-256':
                hash = h.split(':')[1]
        if hash and hash not in self.solvers:
            self.solvers.append(hash)
            self.solvers_lock.acquire()
            p = solvers_dataset_pb2.DataSetInstance()
            p.solver.definition.CopyFrom(solver)
            # p.solver.enviroment_variables (Usamos las variables de entorno por defecto).
            p.hash = hashlib.sha3_256(p.solver.SerializeToString()).hexdigest()
            self.solvers_dataset.data.append(p)
            self._solver.add_solver(solver_with_config=p.solver, solver_config_id=p.hash)
            self.solvers_lock.release()

    def random_service_extended(self):
        config = True
        transport = gateway_pb2.ServiceTransport()
        for hash in self.random_def.hash:
            transport.hash = hash
            if config: # Solo hace falta enviar la configuracion en el primer paquete.
                transport.config.CopyFrom(self.random_config)
                config = False
            yield transport
        transport.ClearField('hash')
        if config: transport.config.CopyFrom(self.random_config)
        transport.service.CopyFrom(self.random_def)
        yield transport

    def init_random_cnf_service(self):
        while True:
            try:
                instance = self.gateway_stub.StartService(self.random_service_extended())
                break
            except grpc.RpcError as e:
                LOGGER('GRPC ERROR.'+ str(e))
                sleep(1)
        uri = instance.instance.uri_slot[0].uri[0]
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
                    request=api_pb2.Empty(),
                    timeout=self.START_AVR_TIMEOUT
                )
            except (grpc.RpcError, TimeoutError) as e:
                if connection_errors < self.CONNECTION_ERRORS:
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

    def updateScore(self, cnf, solver: solvers_dataset_pb2.DataSetInstance, score):
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
        timeout = self.TRAIN_SOLVERS_TIMEOUT
        LOGGER('INICIANDO SERVICIO DE RANDOM CNF')
        self.init_random_cnf_service()
        LOGGER('hecho.')
        while True:
            if self.exit_event.is_set(): break
            if refresh < self.REFRESH:
                LOGGER('REFRESH ES MENOR')
                refresh = refresh + 1
                cnf = self.random_cnf()
                LOGGER('RESPUESTA DEL CNF --> ' + str(cnf))
                is_insat = True  # En caso en que se demuestre lo contrario.
                insats = []  # Solvers que afirman la insatisfactibilidad junto con su respectivo tiempo.
                LOGGER('VAMOS A PROBAR LOS SOLVERS')
                self.solvers_lock.acquire()
                for solver in self.solvers_dataset.data:
                    LOGGER('SOVLER --> ' + str(solver.hash))
                    try:
                        interpretation, time = self._solver.cnf(cnf=cnf, solver_config_id=solver.hash, timeout=timeout)
                    except Exception as e:
                        LOGGER('ERROR SOLVING A CNF'+ str(e))
                        interpretation, time = None, timeout
                        pass
                    if not interpretation or not interpretation.variable:
                        insats.append({
                            'solver': solver,
                            'time': time
                        })
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
                for d in insats:
                    self.updateScore(
                        cnf=cnf,
                        solver=d['solver'],
                        score=(float(+1 / d['time']) if d['time'] != 0 else 1) if is_insat   
                            else (float(-1 / d['time']) if d['time'] != 0 else -1) # comprueba is_insat en cada vuelta, cuando no es necesario, pero el codigo queda más limpio.
                    )
            else:
                LOGGER('ACTUALIZA EL DATASET')
                refresh = 0
                with open(DIR + 'solvers_dataset.bin', 'wb') as file:
                    file.write(self.solvers_dataset.SerializeToString())