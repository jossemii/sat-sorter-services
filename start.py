import logging, celaut_pb2, os, buffer_pb2, gateway_pb2, gateway_pb2_grpc
from threading import Thread
from iterators import TimeoutIterator
from grpcbigbuffer import client_grpc

from iobigdata import IOBigData, mem_manager
from utils import read_file

logging.basicConfig(filename='app.log', level=logging.DEBUG, format='%(asctime)s %(levelname)-8s %(message)s')
LOGGER = lambda message: logging.getLogger().debug(message + '\n')
DIR = '/satsorter/'

REGRESSION_SHA256 = 'fbb4081320fa31de62987b3f82d1f00a490e87e1c7fa497cbf160a94621ab46c'
RANDOM_SHA256 = 'b0fc076a49adb3d5e03b76996cfe82c81efba9f2115d343a39ee46883c5fdc35'

def get_grpc_uri(instance: celaut_pb2.Instance) -> celaut_pb2.Instance.Uri:
    for slot in instance.api.slot:
        #if 'grpc' in slot.transport_protocol.metadata.tag and 'http2' in slot.transport_protocol.metadata.tag:
            # If the protobuf lib. supported map for this message it could be O(n).
        for uri_slot in instance.uri_slot:
            if uri_slot.internal_port == slot.port:
                return uri_slot.uri[0]
    raise Exception('Grpc over Http/2 not supported on this service ' + str(instance))

ENVS = {
    'GATEWAY_MAIN_DIR': '192.168.43.146:8090',
    'SAVE_TRAIN_DATA': 10,
    'MAINTENANCE_SLEEP_TIME': 60,
    'SOLVER_PASS_TIMEOUT_TIMES': 5,
    'SOLVER_FAILED_ATTEMPTS': 5,
    'TRAIN_SOLVERS_TIMEOUT': 30,
    'MAX_REGRESSION_DEGREE': 100,
    'TIME_FOR_EACH_REGRESSION_LOOP': 900,
    'CONNECTION_ERRORS': 20,
    'START_AVR_TIMEOUT': 30,
    'MAX_WORKERS': 20,
    'MAX_REGRESION_WORKERS': 5,
    'MAX_DISUSE_TIME_FACTOR': 1,
    'TIME_SLEEP_WHEN_SOLVER_ERROR_OCCURS': 1,
    'MAX_ERRORS_FOR_SOLVER': 5,
}

import hashlib
# -- The service use sha3-256 for identify internal objects. --
SHA3_256_ID = bytes.fromhex("a7ffc6f8bf1ed76651c14756a061d662f580ff4de43b49fa82d80a4b80f8434a")
SHA3_256 = lambda value: "" if value is None else hashlib.sha3_256(value).digest()


if __name__ == "__main__":

    from time import sleep    
    import train, _get, _solve, regresion
    from threading import get_ident
    import grpc, api_pb2, api_pb2_grpc, solvers_dataset_pb2
    from concurrent import futures
    import grpcbigbuffer as grpcbf
    from api_pb2_grpcbf import UploadService_input_partitions

    
    # Read __config__ file.
    config = api_pb2.celaut__pb2.ConfigurationFile()
    config.ParseFromString(
        read_file('/__config__')
    )    

    gateway_uri = get_grpc_uri(config.gateway)
    ENVS['GATEWAY_MAIN_DIR'] = gateway_uri.ip+':'+str(gateway_uri.port)
    
    """
    for env_var in config.config.enviroment_variables:
        ENVS[env_var] = type(ENVS[env_var])(
            config.config.enviroment_variables[env_var].value
            )
    """

    LOGGER('INIT START THREAD ' + str(get_ident()))

    def unzip_services():
        import zipfile
        with zipfile.ZipFile(DIR + 'services.zip', 'r') as zip_ref: 
            zip_ref.extractall(DIR)
        os.remove(DIR + 'services.zip')
        LOGGER('Services files extracted.')

    def modify_resources_grpcbb(i: dict) -> api_pb2.celaut__pb2.Sysresources:
        return next(
            client_grpc(
                method = gateway_pb2_grpc.GatewayStub(
                            grpc.insecure_channel(ENVS['GATEWAY_MAIN_DIR'])
                        ).ModifyServiceSystemResources,
                input = gateway_pb2.ModifyServiceSystemResourcesInput(
                    min_sysreq = celaut_pb2.Sysresources(
                        mem_limit = i['min']
                    ),
                    max_sysreq = celaut_pb2.Sysresources(
                        mem_limit = i['max']
                    ),
                ),
                partitions_message_mode_parser=True,
                indices_parser = api_pb2.celaut__pb2.Sysresources,
            )
        )

    Thread(target=unzip_services).start()

    IOBigData(
        log = LOGGER,
        ram_pool_method = lambda: config.initial_sysresources.mem_limit,
        modify_resources = lambda d: modify_resources_grpcbb(i=d) 
    )

    grpcbf.modify_env(mem_manager=mem_manager, cache_dir=DIR)

    _regresion = regresion.Session(ENVS=ENVS)
    trainer = train.Session(ENVS=ENVS)
    _solver = _solve.Session(ENVS=ENVS)


    class SolverServicer(api_pb2_grpc.SolverServicer):

        def Solve(self, request_iterator, context):
            cnf = next(grpcbf.parse_from_buffer(
                request_iterator=request_iterator, 
                indices = api_pb2.Cnf,
                partitions_message_mode = True
            ))
            try:
                while True:
                    solver_config_id = _get.cnf(
                        cnf = cnf,
                        tensors = _regresion.get_tensor()
                    )
                    LOGGER('USING SOLVER --> ' + str(solver_config_id))

                    for i in range(ENVS['MAX_ERRORS_FOR_SOLVER']):
                        try:
                            for b in grpcbf.serialize_to_buffer(
                                message_iterator = _solver.cnf(
                                    cnf = cnf,
                                    solver_config_id = solver_config_id
                                )[0],
                                indices = api_pb2.Interpretation
                            ): yield b
                        except Exception as e:
                            LOGGER(str(i) + ' ERROR SOLVING A CNF ON Solve ' + str(e))
                            sleep(ENVS['TIME_SLEEP_WHEN_SOLVER_ERROR_OCCURS'])
                            continue

            except Exception as e:
                LOGGER('Wait more for it, tensor is not ready yet. ')
                for b in grpcbf.serialize_to_buffer(
                    indices = {1: api_pb2.Interpretation, 2: buffer_pb2.Empty}
                ): yield b

        def StreamLogs(self, request_iterator, context):
            if hasattr(self.StreamLogs, 'has_been_called'): 
                raise Exception('Only can call this method once.')
            else: 
                self.StreamLogs.__func__.has_been_called = True
            
            stream_regresion_logs = _regresion.stream_logs()
            with open('app.log') as file:
                while True:
                    try:
                        f = api_pb2.File()
                        f.file = next(file)
                        for b in grpcbf.serialize_to_buffer(message_iterator=f): yield b
                    except: pass
                    for b in grpcbf.serialize_to_buffer(
                            message_iterator = next(TimeoutIterator(
                                stream_regresion_logs(),
                                timeout = 0.2
                            ))
                        ): yield b
                    # TODO Could be async. (needs the async grpc lib.)
                    
        def UploadSolver(self, request_iterator, context):
            LOGGER('New solver ...')
            pit = grpcbf.parse_from_buffer(
                request_iterator = request_iterator,
                partitions_model = UploadService_input_partitions,
                indices = api_pb2.ServiceWithMeta,
                partitions_message_mode = [True, False]
            )
            if next(pit) != api_pb2.ServiceWithMeta: raise Exception('UploadSolver error: this is not a ServiceWithMeta message. '+str(pit))
            trainer.load_solver(
                partition1 = next(pit),
                partition2 = next(pit),
            )
            for b in grpcbf.serialize_to_buffer(): yield b

        def GetTensor(self, request, context):
            tensor_with_ids =  _regresion.get_tensor()
            tensor_with_defitions = api_pb2.Tensor()
            for solver_config_id_tensor in tensor_with_ids.non_escalar.non_escalar:
                tensor_with_defitions.non_escalar.non_escalar.append(
                    api_pb2.Tensor.NonEscalarDimension.NonEscalar(
                        element = _solve.get_solver_with_config(
                            solver_config_id = solver_config_id_tensor.element
                        ),
                        escalar = solver_config_id_tensor.escalar
                    )
                )
            for b in grpcbf.serialize_to_buffer(message_iterator = tensor_with_defitions): yield b

        def StartTrain(self, request, context):
            trainer.start()
            for b in grpcbf.serialize_to_buffer(): yield b

        def StopTrain(self, request, context):
            trainer.stop()
            for b in grpcbf.serialize_to_buffer(): yield b

        # Integrate other tensor
        def AddTensor(self, request_iterator, context):
            tensor = next(grpcbf.parse_from_buffer(request_iterator = request_iterator, indices = api_pb2.Tensor))
            new_data_set = solvers_dataset_pb2.DataSet()
            for solver_with_config_tensor in tensor.non_escalar.non_escalar:
                # Si no se posee ese solver, lo añade y añade, al mismo tiempo, en _solve con una configuración que el trainer considere, 
                #  añade despues la configuración del tensor en la sesion de solve manager (_solve).
                # La configuración que tiene el tensor no será provada por el trainer puesto que este tiene la competencia
                #  de provar las configuraciones que considere.

                solver_config_id = hashlib.sha3_256(
                    solver_with_config_tensor.element.SerializeToString()
                    ).hexdigest()

                _solver.add_solver(
                    solver_with_config = solver_with_config_tensor.element,
                    solver_config_id = solver_config_id,
                    solver_hash = trainer.load_solver(
                                    solver = solver_with_config_tensor.element.definition,
                                    metadata = solver_with_config_tensor.element.meta
                                )
                )

                # Se extraen datos del tensor.
                # Debe de probar el nivel de ajuste que tiene en un punto determinado
                #  para obtener un indice.
                # data.data.update({})
                #TODO
                new_data_set.data.update({
                    solver_config_id : solvers_dataset_pb2.DataSetInstance() # generate_data(solver_with_config_tensor.escalar)
                })

            _regresion.add_data(
                new_data_set = new_data_set
            )
            for b in grpcbf.serialize_to_buffer(): yield b

        def GetDataSet(self, request_iterator, context):
            for b in grpcbf.serialize_to_buffer(
                message_iterator = _regresion.get_data_set()
            ): yield b
        
        # Hasta que se implemente AddTensor.
        def AddDataSet(self, request_iterator, context):
            _regresion.add_data(
                new_data_set = next(grpcbf.parse_from_buffer(
                    request_iterator=request_iterator,
                    indices=api_pb2.solvers__dataset__pb2.DataSet,
                    partitions_message_mode = True
                ))
            )
            for b in grpcbf.serialize_to_buffer(): yield b


    with mem_manager(len = config.initial_sysresources.mem_limit*0.25):
        # create a gRPC server
        server = grpc.server(futures.ThreadPoolExecutor(max_workers=ENVS['MAX_WORKERS']))

        api_pb2_grpc.add_SolverServicer_to_server(
            SolverServicer(), server)

        # Create __solvers__ if it does not exists.
        try:
            os.mkdir(DIR+'__solvers__')
        except:
            # for dev.
            os.system(DIR+'rm -rf __solvers__')
            os.mkdir(DIR+'__solvers__')

        # listen on port 8080
        LOGGER('Starting server. Listening on port 8081.')
        server.add_insecure_port('[::]:8081')
        server.start()
        server.wait_for_termination()
