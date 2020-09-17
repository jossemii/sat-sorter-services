import sys
import os

from flask import Flask, request
import train, select

app = Flask(__name__)


if __name__ == "__main__":

    @app.route('/select', methods=['GET', 'POST'])
    def post_select():
        cnf = request.json.get('cnf')
        solution = select.select(cnf=cnf)
        return {'interpretation':solution}

    @app.route('/train', methods=['GET'])
    def get_train():
        train.start( os.environ['GATEWAY'], os.environ['REFRESH'])

    app.run(host='0.0.0.0', port=8080)