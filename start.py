import logging
logging.basicConfig(filename='app.log', level=logging.DEBUG, format='%(asctime)s %(levelname)-8s %(message)s')
LOGGER = lambda message: logging.getLogger().debug(message)

DIR = ''#'/satrainer/'
GATEWAY = '192.168.1.55:8000' #'172.17.0.1:8000'
SAVE_TRAIN_DATA = 2
MAINTENANCE_SLEEP_TIME = 100
SOLVER_PASS_TIMEOUT_TIMES = 5
SOLVER_FAILED_ATTEMPTS = 5
STOP_SOLVER_TIME_DELTA_MINUTES = 2
TRAIN_SOLVERS_TIMEOUT = 30
MAX_REGRESSION_DEGREE = 100
TIME_FOR_EACH_REGRESSION_LOOP = 999
CONNECTION_ERRORS = 5
START_AVR_TIMEOUT = 30
RANDOM_SERVICE = '79ccccaf235eeeba9197567c38c06f8660c64bf9326d85da25335b165ae71531'

if __name__ == "__main__":

    from time import sleep
    import os
    import json
    from flask import Flask, request
    import train, _get, _solve
    from threading import get_ident, Thread
    import regresion
    import grpc, service_pb2, service_pb2_grpc, futures

    try:
        GATEWAY = os.environ['GATEWAY']
    except KeyError:
        pass
    try:
        SAVE_TRAIN_DATA = os.environ['SAVE_TRAIN_DATA']
    except KeyError:
        pass
    try:
        MAINTENANCE_SLEEP_TIME = os.environ['MAINTENANCE_SLEEP_TIME']
    except KeyError:
        pass
    try:
        SOLVER_PASS_TIMEOUT_TIMES = os.environ['SOLVER_PASS_TIMEOUT_TIMES']
    except KeyError:
        pass
    try:
        SOLVER_FAILED_ATTEMPTS = os.environ['SOLVER_FAILED_ATTEMPTS']
    except KeyError:
        pass
    try:
        STOP_SOLVER_TIME_DELTA_MINUTES = os.environ['STOP_SOLVER_TIME_DELTA_MINUTES']
    except KeyError:
        pass
    try:
        TRAIN_SOLVERS_TIMEOUT = os.environ['TRAIN_SOLVERS_TIMEOUT']
    except KeyError:
        pass
    try:
        MAX_REGRESSION_DEGREE = os.environ['MAX_REGRESSION_DEGREE']
    except KeyError:
        pass
    try:
        TIME_FOR_EACH_REGRESSION_LOOP = os.environ['TIME_FOR_EACH_REGRESSION_LOOP']
    except KeyError:
        pass
    try:
        CONNECTION_ERRORS = os.environ['CONNECTION_ERRORS']
    except KeyError:
        pass
    try:
        START_AVR_TIMEOUT = os.environ['START_AVR_TIMEOUT']
    except KeyError:
        pass

    LOGGER('INIT START THREAD ' + str(get_ident()))
    Thread(target=regresion.init, name='Regression').start()
    trainer = train.Session()
    _solver = _solve.Session()

    class SolverServicer(service_pb2_grpc.SolverServicer): 

        def Solve(self, request, context):
            cnf = request.get_json()['cnf']
            solver = _get.cnf(
                cnf=request
            )
            LOGGER('USING SOLVER --> '+ str(solver))
            return _solver.cnf(cnf=cnf, solver=solver)[0]

        def StreamLogs(self, request, context):
            with open('app.log') as file:
                while True:
                    f = service_pb2.File()
                    f.file = file.read()
                    yield f
                    sleep(1)
            
        def UploadSolver(self, request, context):
            trainer.load_solver(request)
            return service_pb2.WhoAreYourParams()

        def GetTensor(self, request, context):
            with open(DIR + 'tensors', 'r') as file:
                while True:
                    yield file

        def StartTrain(self, request, context):
            trainer.start()
            return service_pb2.WhoAreYourParams()

        def StopTrain(self, request, context):
            trainer.stop()
            return service_pb2.WhoAreYourParams()


    # create a gRPC server
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    
    service_pb2_grpc.add_SolverServicer_to_server(
            SolverServicer(), server)

    # listen on port 8080
    LOGGER('Starting server. Listening on port 8080.')
    server.add_insecure_port('[::]:8080')
    server.start()
    server.wait_for_termination()
