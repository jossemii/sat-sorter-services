import os

from flask import Flask, request
import train, _get

app = Flask(__name__)

if __name__ == "__main__":

    @app.route('/select', methods=['GET', 'POST'])
    def _select():
        return { 'interpretation': _get.cnf(
            cnf=request.get_json()['cnf']
        ) }

    @app.route('/train/start', methods=['GET'])
    def start_train():
        train.Session.__call__().init( os.environ['GATEWAY'] ) # subprocess
        # return 'DoIt'

    @app.route('/train/stop', methods=['GET'])
    def stop_train():
        train.Session.__call__().stop()
        return 'DoIt'

    app.run(host='0.0.0.0', port=8080)
