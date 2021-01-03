DIR = ''  # '/satrainer/'
GATEWAY = '192.168.1.65:8000'
SAVE_TRAIN_DATA = 2
MAINTENANCE_SLEEP_TIME = 10
SOLVER_PASS_TIMEOUT_TIMES = 5
SOLVER_FAILED_ATTEMPTS = 5
STOP_SOLVER_TIME_DELTA_MINUTES = 2
TRAIN_SOLVERS_TIMEOUT = 30
MAX_REGRESSION_DEGREE = 100

if __name__ == "__main__":

    import os
    import json
    from flask import Flask, request
    import train, _get, _solve
    from threading import get_native_id

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

    print('INIT START THREAD ', get_native_id())
    app = Flask(__name__)
    trainer = train.Session()
    _solver = _solve.Session()


    @app.route('/solve', methods=['GET', 'POST'])
    def solve():
        cnf = request.get_json()['cnf']
        solver = _get.cnf(
            cnf=cnf
        )
        return {
            'interpretation': _solver.cnf(cnf=cnf, solver=solver)[0]
        }


    @app.route('/upsolver', methods=['GET', 'POST'])
    def up_solver():
        trainer.load_solver(request.get_json()['solver'])


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
