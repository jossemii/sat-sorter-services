import shutil
from gateway_pb2_grpcbf import StartService_input, StartService_input_partitions
import regresion
from threading import get_ident, Thread, Lock
import grpcbigbuffer as grpcbf
import grpc
from time import sleep
import api_pb2, api_pb2_grpc, solvers_dataset_pb2, gateway_pb2, gateway_pb2_grpc, celaut_pb2 as celaut
from singleton import Singleton
import _solve
from start import LOGGER, DIR, SHA3_256, SHA3_256_ID, get_grpc_uri
from utils import read_file
from grpcbigbuffer import Dir, client_grpc


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
        
        self.random_hashes=[
            gateway_pb2.celaut__pb2.Any.Metadata.HashTag.Hash(
                type = bytes.fromhex("a7ffc6f8bf1ed76651c14756a061d662f580ff4de43b49fa82d80a4b80f8434a"),
                value = bytes.fromhex("b11074a440261eee100bb8751610be5c5cf6efdf9a9a1128de7d61dd93dc0fd9")
            )
        ]
        self.random_stub = None
        self.random_token = None
        self.solvers_dataset = solvers_dataset_pb2.DataSet()
        self.solvers = []  # Lista de los solvers por hash.
        self.solvers_dataset_lock = Lock()  # Se usa al añadir un solver y durante cada iteracion de entrenamiento.
        self.solvers_lock = Lock()  # Se uso al añadir un solver ya que podrían añadirse varios concurrentemente.
        self.do_stop = False
        self._solver = _solve.Session(ENVS=ENVS)  # Using singleton pattern.
        self._regresion = regresion.Session(ENVS=ENVS)  # Using singleton pattern.

        # Random CNF Service.
        self.random_config = celaut.Configuration()

    def stop_random(self):
        LOGGER('Stopping random service.')
        while True:
            try:
                next(client_grpc(
                    method = self.gateway_stub.StopService,
                    input = gateway_pb2.TokenMessage(
                            token = self.random_token
                        )
                ))
                break
            except grpc.RpcError as e:
                LOGGER('GRPC ERROR STOPPING RANDOM ' + str(e))
                sleep(1)

    def stop(self):
        # Para evitar que la instrucción stop mate el hilo durante un entrenamiento,
        #  y deje instancias fuera de pila y por tanto servicios zombie, el método stop 
        #  emite el mensaje de que se debe de parar el entrenamiento en la siguiente vuelta, 
        #  dando uso de do_stop=True, y a continuación espera a que el init salga del while 
        #  con thread.join().
        if not self.do_stop and self.thread:
            LOGGER('Stopping train.')
            self.do_stop = True
            self.thread.join()
            self.stop_random()
            self.do_stop = False
            self.thread = None

    def load_solver(self, partition1: api_pb2.solvers__dataset__pb2.SolverWithConfig, partition2: str) -> str:
        
        # Se puede cargar un solver sin estar completo, 
        #  pero debe de contener si o si la sha3-256
        #  ya que el servicio no la calculará (ni comprobará).

        # En caso de no estar completo ni poseer la SHA3_256 identificará el servicio mediante la hash de su version incompleta. 
        # Si más tarde se vuelve a subir el servicio incompleto, no se podrá detectar que es el mismo y se entrenarán ambos por separado.
        # Esto podría crear duplicaciones en el tensor, pero no debería suponer ningun tipo de error, solo ineficiencia.

        solver_hash = None
        metadata =partition1.metadata
        solver = partition1.solver
        for h in metadata.hashtag.hash:
            if h.type == SHA3_256_ID:
                solver_hash = h.value.hex()   

        solver_hash = SHA3_256(
            value = grpcbf.partitions_to_buffer(
                message = api_pb2.ServiceWithMeta,
                partitions = (
                    partition1,
                    partition2,
                ),
                partitions_model = StartService_input_partitions[4]
            )
        ) if not solver_hash else solver_hash

        self.solvers_lock.acquire()
        if solver_hash and solver_hash not in self.solvers:
            self.solvers.append(solver_hash)
            shutil.move(partition2, '__solvers__/'+solver_hash+'/p2')
            with open(DIR + '__solvers__/'+solver_hash+'/p1', 'wb') as file:
                file.write(partition1.SerializeToString())

            # En este punto se pueden crear varias versiones del mismo solver, 
            # con distintas variables de entorno.
            self.solvers_dataset_lock.acquire()
            p = solvers_dataset_pb2.SolverWithConfig()
            p.definition.CopyFrom(solver)
            p.meta.CopyFrom(metadata)
            # p.enviroment_variables (Usamos las variables de entorno por defecto).
            solver_with_config_hash = SHA3_256(
                    value = p.SerializeToString()
                ).hex() # This service not touch metadata, so it can use the hash for id.
            self.solvers_dataset.data[solver_with_config_hash].CopyFrom(solvers_dataset_pb2.DataSetInstance())
            self._solver.add_solver(
                solver_with_config = p, 
                solver_config_id = solver_with_config_hash,
                solver_hash = solver_hash
            )
            self.solvers_dataset_lock.release()
            
        self.solvers_lock.release()
        return solver_hash

    def clear_dataset(self) -> None:
        self.solvers_dataset_lock.acquire()
        for solver in self.solvers_dataset.data.values():
            solver.ClearField('data')
        self.solvers_dataset_lock.release()

    def random_service_extended(self):
        config = True
        for hash in self.random_hashes:
            if config:  # Solo hace falta enviar la configuracion en el primer paquete.
                config = False
                yield gateway_pb2.HashWithConfig(
                    hash = hash,
                    config = self.random_config
                )
            yield hash
        yield (gateway_pb2.ServiceWithMeta, Dir(DIR + 'random.service'))

    def init_random_cnf_service(self):
        LOGGER('Launching random service instance.')
        while True:
            try:
                instance = next(client_grpc(
                    method = self.gateway_stub.StartService,
                    input = self.random_service_extended(),
                    indices_parser = gateway_pb2.Instance,
                    partitions_message_mode_parser=True,
                    indices_serializer = StartService_input
                ))
                break
            except grpc.RpcError as e:
                LOGGER('GRPC ERROR.' + str(e))
                sleep(1)
                
        uri = get_grpc_uri(instance.instance)
        self.random_stub = api_pb2_grpc.RandomStub(
            grpc.insecure_channel(
                uri.ip + ':' + str(uri.port)
            )
        )
        self.random_token = instance.token

    def random_cnf(self) -> api_pb2.Cnf:
        connection_errors = 0
        while True:
            try:
                LOGGER('OBTENIENDO RANDON CNF')
                return next(client_grpc(
                    method = self.random_stub.RandomCnf,
                    input = api_pb2.Empty(),
                    indices_parser = api_pb2.Cnf,
                    partitions_message_mode_parser = True,
                    # timeout = self.START_AVR_TIMEOUT
                ))
            except (grpc.RpcError, TimeoutError, Exception) as e:
                if connection_errors < self.CONNECTION_ERRORS:
                    connection_errors = connection_errors + 1
                    sleep(1)  # Evita condiciones de carrera si lo ejecuta tras recibir la instancia.
                    continue
                else:
                    connection_errors = 0
                    LOGGER('  ERROR OCCURS OBTAINING THE CNF --> ' + str(e))
                    LOGGER('VAMOS A CAMBIAR EL SERVICIO DE OBTENCION DE CNFs RANDOM')
                    self.stop_random()
                    self.init_random_cnf_service()
                    LOGGER('listo. ahora vamos a probar otra vez.')
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
        if type_of_cnf not in solver.data:
            solver.data[type_of_cnf].index = 1
            solver.data[type_of_cnf].score = 0
        solver.data[type_of_cnf].score = (solver.data[type_of_cnf].score * solver.data[type_of_cnf].index + score) / (
                    solver.data[type_of_cnf].index + 1)
        solver.data[type_of_cnf].index = solver.data[type_of_cnf].index + 1

    def start(self):
        if self.thread or self.do_stop: return None
        try:
            self.thread = Thread(target = self.init, name = 'Trainer')
            self.thread.start()
        except RuntimeError:
            LOGGER('Error: train thread was started and have an error.')

    def init(self):
        LOGGER('INICIANDO TRAINER, THREAD IS ' + str(get_ident()))
        refresh = 0
        timeout = self.TRAIN_SOLVERS_TIMEOUT
        LOGGER('INICIANDO SERVICIO DE RANDOM CNF')
        self.init_random_cnf_service()
        LOGGER('hecho.')
        # Si se emite una solicitud para detener el entrenamiento el hilo 
        #  finalizará en la siguiente iteración.
        while not self.do_stop:
            if refresh < self.REFRESH:
                LOGGER('REFRESH ES MENOR')
                refresh = refresh + 1
                cnf = self.random_cnf()
                LOGGER('OBTENIDO NUEVO CNF. ')
                is_insat = True  # En caso en que se demuestre lo contrario.
                insats = []  # Solvers que afirman la insatisfactibilidad junto con su respectivo tiempo.
                LOGGER('VAMOS A PROBAR LOS SOLVERS')
                self.solvers_dataset_lock.acquire()
                for hash, solver_data in self.solvers_dataset.data.items():
                    if self.do_stop: break
                    LOGGER('SOLVER --> ' + str(hash))
                    try:
                        interpretation, time = self._solver.cnf(cnf=cnf, solver_config_id=hash, timeout=timeout)
                    except Exception as e:
                        LOGGER('INTERNAL ERROR SOLVING A CNF ON TRAIN ' + str(e))
                        interpretation, time = None, timeout
                        pass
                    # Durante el entrenamiento, si ha ocurrido un error al obtener un cnf se marca como insatisfactible,
                    # tras muchas iteraciones no debería suponer un problema en el tensor.
                    if not interpretation or not interpretation.satisfiable or len(interpretation.variable) == 0:
                        insats.append({
                            'solver': solver_data,
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
                            solver=solver_data,
                            score=score
                        )
                self.solvers_dataset_lock.release()
                # Registra los solvers que afirmaron la insatisfactibilidad en caso en que ninguno
                #  haya demostrado lo contrario.
                for d in insats:
                    self.updateScore(
                        cnf=cnf,
                        solver=d['solver'],
                        score=(float(+1 / d['time']) if d['time'] != 0 else 1) if is_insat
                        else (float(-1 / d['time']) if d['time'] != 0 else -1)
                        # comprueba is_insat en cada vuelta, cuando no es necesario, pero el codigo queda más limpio.
                    )
            else:
                LOGGER('ACTUALIZA EL DATASET')
                refresh = 0
                self._regresion.add_data(new_data_set = self.solvers_dataset)
                # No formatear los datos cada vez provocaría que el regresion realizara equívocamente la media, pues 
                # estaría contando los datos anteriores una y otra vez.
                self.clear_dataset()
