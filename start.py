import logging, hyweb_pb2
from threading import Thread
from iterators import TimeoutIterator

logging.basicConfig(filename='app.log', level=logging.DEBUG, format='%(asctime)s %(levelname)-8s %(message)s')
LOGGER = lambda message: logging.getLogger().debug(message + '\n')
DIR = '/satsorter/'

def get_grpc_uri(instance: hyweb_pb2.Instance) -> hyweb_pb2.Instance.Uri:
    for slot in instance.api.slot:
        if 'grpc' in slot.transport_protocol.hashtag.tag and 'http2' in slot.transport_protocol.hashtag.tag:
            # If the protobuf lib. supported map for this message it could be O(n).
            for uri_slot in instance.uri_slot:
                if uri_slot.internal_port == slot.port:
                    return uri_slot.uri[0]
    raise Exception('Grpc over Http/2 not supported on this service ' + str(instance))

ENVS = {
    'GATEWAY_MAIN_DIR': '',
    'SAVE_TRAIN_DATA': 10,
    'MAINTENANCE_SLEEP_TIME': 60,
    'SOLVER_PASS_TIMEOUT_TIMES': 5,
    'SOLVER_FAILED_ATTEMPTS': 5,
    'TRAIN_SOLVERS_TIMEOUT': 30,
    'MAX_REGRESSION_DEGREE': 100,
    'TIME_FOR_EACH_REGRESSION_LOOP': 900,
    'CONNECTION_ERRORS': 5,
    'START_AVR_TIMEOUT': 30,
    'MAX_WORKERS': 20,
    'MAX_REGRESION_WORKERS': 5,
    'MAX_DISUSE_TIME_FACTOR': 1,
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
    from maintainer import maintainer

    # Read __config__ file.
    config = api_pb2.hyweb__pb2.ConfigurationFile()
    config.ParseFromString(
        open('/__config__', 'rb').read()
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
      
    _regresion = regresion.Session(ENVS=ENVS)
    trainer = train.Session(ENVS=ENVS)
    _solver = _solve.Session(ENVS=ENVS)
    Thread(target=maintainer, name='Maintainer', args=(ENVS, LOGGER, )).start()


    class SolverServicer(api_pb2_grpc.SolverServicer):

        def Solve(self, request, context):
            try:
                solver_with_config = _get.cnf(
                    cnf = request,
                    tensors = _regresion.get_tensor()
                )
            except:
                LOGGER('Wait more for it, tensor is not ready yet. ')
                return api_pb2.Empty()

            solver_config_id = SHA3_256(
                value = solver_with_config.SerializeToString()
            ).hex()
            LOGGER('USING SOLVER --> ' + str(solver_config_id))
            for i in range(5):
                try:
                    return _solver.cnf(
                        cnf = request,
                        solver_config_id = solver_config_id,
                        solver_with_config = solver_with_config
                    )[0]
                except Exception as e:
                    LOGGER(str(i) + ' ERROR SOLVING A CNF ON Solve ' + str(e))
                    sleep(1)
                    continue
            raise Exception

        def StreamLogs(self, request, context):
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
                        yield f
                    except: pass
                    yield next(TimeoutIterator(
                        stream_regresion_logs(),
                        timeout = 0.2
                        ))
                    # TODO Could be async. (needs the async grpc lib.)
                    
        def UploadSolver(self, request, context):
            trainer.load_solver(request)
            return api_pb2.Empty()

        def GetTensor(self, request, context):
            return _regresion.get_tensor()

        def StartTrain(self, request, context):
            trainer.start()
            return api_pb2.Empty()

        def StopTrain(self, request, context):
            trainer.stop()
            return api_pb2.Empty()

        # Integrate other tensor
        def AddTensor(self, request, context):
            new_data_set = solvers_dataset_pb2.DataSet()
            for t in request.tensor:
                hash = hashlib.sha3_256(t.element.SerializeToString()).hexdigest()
                data =  solvers_dataset_pb2.DataSetInstance()
                try:
                    data.solver.CopyFrom( t.element )
                except:
                    LOGGER('El elemento del tensor parede no ser un SolverWithConfig.')

                # Se extraen datos del tensor.
                # Debe de probar el nivel de ajuste que tiene en un punto determinado
                #  para obtener un indice.
                # data.data.update({})
                #TODO

                new_data_set.data.update({
                    hash : data
                })

            _regresion.add_data( #TODO
                new_data_set = new_data_set
            )
            return api_pb2.Empty()

        def GetDataSet(self, request, context):
            return _regresion.get_data_set()
        
        # Hasta que se implemente AddTensor.
        def AddDataSet(self, request, context):
            _regresion.add_data(new_data_set = request)
            return api_pb2.Empty()


    # create a gRPC server
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=ENVS['MAX_WORKERS']))

    api_pb2_grpc.add_SolverServicer_to_server(
        SolverServicer(), server)

    # listen on port 8080
    LOGGER('Starting server. Listening on port 8080.')
    server.add_insecure_port('[::]:8080')
    server.start()
    server.wait_for_termination()
