import logging, hyweb_pb2

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
    'SAVE_TRAIN_DATA': 2,
    'MAINTENANCE_SLEEP_TIME': 100,
    'SOLVER_PASS_TIMEOUT_TIMES': 5,
    'SOLVER_FAILED_ATTEMPTS': 5,
    'TRAIN_SOLVERS_TIMEOUT': 30,
    'MAX_REGRESSION_DEGREE': 100,
    'TIME_FOR_EACH_REGRESSION_LOOP': 900,
    'CONNECTION_ERRORS': 5,
    'START_AVR_TIMEOUT': 30,
    'MAX_WORKERS': 20,
    'MAX_DISUSE_TIME_FACTOR': 1,
}

import hashlib
# -- The service use sha3-256 for identify internal objects. --
SHA3_256_ID = bytes.fromhex("a7ffc6f8bf1ed76651c14756a061d662f580ff4de43b49fa82d80a4b80f8434a")
SHA3_256 = lambda value: "" if value is None else hashlib.sha3_256(value).digest()


if __name__ == "__main__":

    from time import sleep    
    import train, _get, _solve
    from threading import get_ident, Thread
    import regresion
    import grpc, api_pb2, api_pb2_grpc
    from concurrent import futures
    
    # Read __config__ file.
    config = api_pb2.ipss__pb2.ConfigurationFile()
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
      
    Thread(target=regresion.init, name='Regression', args=(ENVS,)).start()   # Cuando se añadan los paquetes necesarios al .service/Dockerfile   
    trainer = train.Session(ENVS=ENVS)
    _solver = _solve.Session(ENVS=ENVS)


    class SolverServicer(api_pb2_grpc.SolverServicer):

        def Solve(self, request, context):
            try:
                solver_with_config = _get.cnf(
                    cnf=request
                )
            except FileNotFoundError:
                LOGGER('Wait more for it, tensor is not ready. ')
                return api_pb2.Empty()

            solver_config_id = SHA3_256(
                value = solver_with_config.SerializeToString()
            ).hex()
            LOGGER('USING SOLVER --> ' + str(solver_config_id))
            while True:
                try:
                    return _solver.cnf(
                        cnf=request,
                        solver_config_id=solver_config_id,
                        solver_with_config=solver_with_config
                    )[0]
                except Exception as e:
                    LOGGER('ERROR SOLVING A CNF ON Solve ' + str(e))
                    continue

        def StreamLogs(self, request, context):
            with open('app.log') as file:
                while True:
                    f = api_pb2.File()
                    f.file = file.read()
                    yield f
                    sleep(1)

        def UploadSolver(self, request, context):
            trainer.load_solver(request)
            return api_pb2.Empty()

        def GetTensor(self, request, context):
            with open(DIR + 'tensor.onnx', 'rb') as file:
                while True:
                    tensor = api_pb2.onnx__pb2.ONNX()
                    tensor.ParseFromString(file.read())
                    yield tensor
                    sleep(ENVS['TIME_FOR_EACH_REGRESSION_LOOP'])

        def StartTrain(self, request, context):
            trainer.start()
            return api_pb2.Empty()

        def StopTrain(self, request, context):
            trainer.stop()
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
