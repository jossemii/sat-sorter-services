DIR = ''  # '/satrainer/'
MAX_REGRESSION_DEGREE = 100
GATEWAY = '192.168.1.64:8000'
SAVE_TRAIN_DATA = 2
MAINTENANCE_SLEEP_TIME = 10
SOLVER_PASS_TIMEOUT_TIMES = 5
SOLVER_FAILED_ATTEMPTS = 5
STOP_SOLVER_TIME_DELTA_MINUTES = 2
TRAIN_SOLVERS_TIMEOUT = 30

if __name__ == "__main__":

    import os
    import json
    from flask import Flask, request
    import train, _get, _solve
    from threading import get_native_id

    print('INIT START THREAD ', get_native_id())
    app = Flask(__name__)
    trainer = train.Session()
    _solver = _solve.Session()

    try:
        GATEWAY = os.environ['GATEWAY']
    except KeyError:
        pass


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
