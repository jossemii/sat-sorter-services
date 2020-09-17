from flask import Flask, request
import train

app = Flask(__name__)


def select(cnf):
    solvers = open('solvers.json', 'r')
    best_interpretation, best_interpretation_score = None, 0
    for solver in solvers:
        if cnf in solver and best_interpretation_score<solver[cnf]['score']:
            best_interpretation, best_interpretation_score = solver, solver[cnf]['score']
    return best_interpretation


if __name__ == "__main__":

    @app.route('/', methods=['GET', 'POST'])
    def post():
        cnf = request.json.get('cnf')
        solution = select(cnf=cnf)
        return {'interpretation':solution}

    app.run(host='0.0.0.0', port=8080)