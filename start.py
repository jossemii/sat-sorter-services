from flask import Response, stream_with_context
import logging
logging.basicConfig(filename='app.log', level=logging.DEBUG, format='%(asctime)s %(levelname)-8s %(message)s')
LOGGER = lambda message: logging.getLogger().debug(message)

DIR = ''  # '/satrainer/'
GATEWAY = '192.168.1.65:8000'
SAVE_TRAIN_DATA = 2
MAINTENANCE_SLEEP_TIME = 100
SOLVER_PASS_TIMEOUT_TIMES = 5
SOLVER_FAILED_ATTEMPTS = 5
STOP_SOLVER_TIME_DELTA_MINUTES = 2
TRAIN_SOLVERS_TIMEOUT = 30
MAX_REGRESSION_DEGREE = 100
TIME_FOR_EACH_REGRESSION_LOOP = 9999
CONNECTION_ERRORS = 5
START_AVR_TIMEOUT = 30

if __name__ == "__main__":

    from time import sleep
    import os
    import json
    from flask import Flask, request
    import train, _get, _solve
    from threading import get_ident, Thread
    import regresion

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
    app = Flask(__name__)
    Thread(target=regresion.init, name='Regression').start()
    trainer = train.Session()
    _solver = _solve.Session()


    @app.route('/solve', methods=['GET', 'POST'])
    def solve():
        cnf = request.get_json()['cnf']
        solver = _get.cnf(
            cnf=cnf
        )
        LOGGER('USING SOLVER --> '+ str(solver))
        interpretation = _solver.cnf(cnf=cnf, solver=solver)[0]
        return {
            'interpretation': interpretation
        }


    @app.route('/stream', methods=['GET'])
    def stream():
        def generate():
            with open('app.log') as f:
                while True:
                    yield f.read()
                    sleep(1)

        return Response(stream_with_context(generate()))


    @app.route('/upsolver', methods=['GET', 'POST'])
    def up_solver():
        trainer.load_solver(request.get_json()['solver'])
        return 'DoIt'


    @app.route('/tensor', methods=['GET'])
    def get_tensor():
        with open(DIR + 'tensors.json', 'r') as file:
            return json.load(file)


    @app.route('/train/start', methods=['GET'])
    def start_train():
        trainer.start()
        return 'DoIt'


    @app.route('/train/stop', methods=['GET'])
    def stop_train():
        trainer.stop()
        return 'DoIt'


    app.run(host='0.0.0.0', port=8080)
