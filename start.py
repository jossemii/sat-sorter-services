import sys
import os

from flask import Flask, request
import train, _select

app = Flask(__name__)

if __name__ == "__main__":

    @app.route('/select', methods=['GET', 'POST'])
    def _select():
        print( request.get_json()['cnf'] )
        return { 'interpretation': _select.cnf() }

    @app.route('/train', methods=['GET'])
    def start_train():
        train.Session.__call__.__init__(
            os.environ['GATEWAY'],
            os.environ['REFRESH']
        )

    @app.route('/train/stop', methods=['GET'])
    def stop_train():
        train.Session.__call__.stop()

    app.run(host='0.0.0.0', port=8080)