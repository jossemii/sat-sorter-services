import logging
logging.basicConfig(filename='app.log', level=logging.DEBUG, format='%(asctime)s %(levelname)-8s %(message)s')
LOGGER = lambda message: logging.getLogger().debug(message)
DIR = '' #DIR = '/satrainer/'

ENVS = {
    'GATEWAY_MAIN_DIR' : '', # Lista de direcciones para gateway.
    'SAVE_TRAIN_DATA' : 2,
    'MAINTENANCE_SLEEP_TIME' : 100,
    'SOLVER_PASS_TIMEOUT_TIMES' : 5,
    'SOLVER_FAILED_ATTEMPTS' : 5,
    'STOP_SOLVER_TIME_DELTA_MINUTES' : 2,
    'TRAIN_SOLVERS_TIMEOUT' : 30,
    'MAX_REGRESSION_DEGREE' : 100,
    'TIME_FOR_EACH_REGRESSION_LOOP' : 50,
    'CONNECTION_ERRORS' : 5,
    'START_AVR_TIMEOUT' : 30 
}


if __name__ == "__main__":

    from time import sleep
    import os, hashlib
    import train, _get, _solve
    from threading import get_ident, Thread
    import regresion
    import grpc, api_pb2, api_pb2_grpc
    from concurrent import futures

    # Read __config__ file.
    #config = api_pb2.ipss__pb2.ConfigurationFile()
    #config.ParseFromString(
    #    open('/__config__', 'rb').read()
    #)

    #gateway_uri = api_pb2.ipss__pb2.Instance.Uri()
    #gateway_uri = config.gateway.uri_slot[0].uri[0]
    #ENVS['GATEWAY_MAIN_DIR'] = gateway_uri.ip+':'+str(gateway_uri.port)

    ENVS['GATEWAY_MAIN_DIR'] = '192.168.1.144:8080' # <- QUITAR ESTO.

    #for env_var in config.config.enviroment_variables:
    #    ENVS[env_var] = type(ENVS[env_var])(
    #        config.config.enviroment_variables[env_var].value
    #        )

    LOGGER('INIT START THREAD ' + str(get_ident()))
      
    Thread(target=regresion.init, name='Regression', args=(ENVS,)).start()   # Cuando se añadan los paquetes necesarios al .service/Dockerfile   
    trainer = train.Session(ENVS=ENVS)
    _solver = _solve.Session(ENVS=ENVS)

    class SolverServicer(api_pb2_grpc.SolverServicer): 

        def Solve(self, request, context):
            solver_with_config = _get.cnf(
                cnf=request
            )
            solver_config_id = hashlib.sha256(solver_with_config.SerializeToString())
            LOGGER('USING SOLVER --> '+ str(solver_config_id))
            return _solver.cnf(cnf=request, solver_config_id=solver_config_id)[0]

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
            with open(ENVS['DIR'] + 'tensor.onnx', 'rb') as file:
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
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    
    api_pb2_grpc.add_SolverServicer_to_server(
            SolverServicer(), server)

    # listen on port 8080
    LOGGER('Starting server. Listening on port 8080.')
    server.add_insecure_port('[::]:8080')
    server.start()
    server.wait_for_termination()
