import hashlib
import os
from threading import Thread

from celaut_framework.dependency_manager.dependency_manager import DependencyManager
from iterators import TimeoutIterator


from celaut_framework.resource_manager.resourcemanager import ResourceManager, mem_manager
from src.envs import ENVS, LOGGER, DIR, DEV_ENVS, DEV_MODE
from src.utils.modify_resources import MODIFY_SYSTEM_RESOURCES_LAMBDA
from src.utils.utils import read_file, get_grpc_uri

if __name__ == "__main__":

    from time import sleep
    from src.solve import _solve, _get
    from src.train import train
    from threading import get_ident
    import grpc
    from protos import api_pb2, api_pb2_grpc, solvers_dataset_pb2
    from celaut_framework.gateway.protos import buffer_pb2
    from src.regresion import regresion
    from concurrent import futures
    import grpcbigbuffer as grpcbf
    from protos.api_pb2_grpcbf import UploadService_input_partitions

    # Read __config__ file.
    if not DEV_MODE:
        config = api_pb2.celaut__pb2.ConfigurationFile()
        config.ParseFromString(
            read_file('/__config__')
        )

        gateway_uri = get_grpc_uri(config.gateway)
        ENVS['GATEWAY_MAIN_DIR'] = gateway_uri.ip+':'+str(gateway_uri.port)
        mem_limit: int = config.initial_sysresources.mem_limit

        """
        for env_var in config.config.enviroment_variables:
            ENVS[env_var] = type(ENVS[env_var])(
                config.config.enviroment_variables[env_var].value
                )
        """

    else:
        ENVS['GATEWAY_MAIN_DIR'] = DEV_ENVS['GATEWAY_MAIN_DIR']
        mem_limit: int = DEV_ENVS['MEM_LIMIT']

    LOGGER('INIT START THREAD ' + str(get_ident()))

    def unzip_services():
        import zipfile
        with zipfile.ZipFile(DIR + 'services.zip', 'r') as zip_ref: 
            zip_ref.extractall(DIR)
        os.remove(DIR + 'services.zip')
        LOGGER('Services files extracted.')

    if not DEV_MODE: Thread(target=unzip_services).start()

    ResourceManager(
        log = LOGGER,
        ram_pool_method = lambda: mem_limit,
        modify_resources = MODIFY_SYSTEM_RESOURCES_LAMBDA
    )

    DependencyManager(
        gateway_main_dir = ENVS['GATEWAY_MAIN_DIR'],
        maintenance_sleep_time = ENVS['MAINTENANCE_SLEEP_TIME'],
        timeout = ENVS['TRAIN_SOLVERS_TIMEOUT'],
        failed_attempts = ENVS['SOLVER_FAILED_ATTEMPTS'],
        pass_timeout_times = ENVS['SOLVER_PASS_TIMEOUT_TIMES'],
        dev_client = DEV_ENVS['CLIENT_ID'] if DEV_MODE else None,
        static_service_directory = DIR,
        dynamic_service_directory = DIR+'__services__/'
    )

    grpcbf.modify_env(mem_manager=mem_manager, cache_dir=DIR)

    _regresion = regresion.Session(
        time_for_each_regression_loop = ENVS['TIME_FOR_EACH_REGRESSION_LOOP']
    )
    trainer = train.Session(
        save_train_data = ENVS['SAVE_TRAIN_DATA'],
        train_solvers_timeout = ENVS['TRAIN_SOLVERS_TIMEOUT'],
        time_for_each_regression_loop=ENVS['TIME_FOR_EACH_REGRESSION_LOOP']
    )
    _solver = _solve.Session()


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
                                stream_regresion_logs,
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
            if next(pit) != api_pb2.ServiceWithMeta: raise Exception('UploadSolver error: this is not a ServiceWithMeta message. ' + str(pit))
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
                        element = DependencyManager().get_service_with_config(
                            service_config_id = solver_config_id_tensor.element
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
                #  añade despues la configuración del tensor en la session de solve manager (_solve).
                # La configuración que tiene el tensor no será probada por el trainer, puesto que este tiene la competencia
                #  de probar las configuraciones que considere.

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


    with mem_manager(len = mem_limit*0.25):
        # create a gRPC server
        server = grpc.server(futures.ThreadPoolExecutor(max_workers=ENVS['MAX_WORKERS']))

        api_pb2_grpc.add_SolverServicer_to_server(
            SolverServicer(), server)

        # Create __services__ if it does not exists.
        try:
            os.mkdir(DIR+'__services__')
        except:
            # for dev.
            os.system(DIR+'rm -rf __services__')
            os.mkdir(DIR+'__services__')

        # listen on port 8080
        LOGGER('Starting server. Listening on port 8081.')
        server.add_insecure_port('[::]:8081')
        server.start()
        server.wait_for_termination()
