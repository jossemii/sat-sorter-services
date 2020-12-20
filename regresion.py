from time import sleep
import numpy as np
from sklearn.linear_model import LinearRegression
from sklearn.preprocessing import PolynomialFeatures

import json
DIR = '' # /satrainer/
MAX_DEGREE = 100


def regression_with_degree(degree: int, input: np.array, output: np.array):
    input = PolynomialFeatures(degree= degree, include_bias=False).fit_transform(input)

    # Create a model of regression.
    model = LinearRegression().fit(input, output )
    return {
        'tensor params': model.intercept_.tolist()+model.coef_[0].tolist(),
        'coefficient of determination': model.score(input, output)
    }

def solver_regression(solver: dict):
    # Get input variables. Num of cnf variables and Num of cnf clauses.
    input = np.array(
        [[int(var) for var in value.split(':')] for value in solver]
    ).reshape(-1, 2)

    # Get output variable. Score.
    output = np.array(
        [value.get('score') or None for value in solver.values()]
    ).reshape(-1, 1)

    best_tensor = {'coefficient of determination': 0}
    for degree in range(1, MAX_DEGREE+1):
        tensor = regression_with_degree(degree= degree, input=input, output=output)
        if tensor['coefficient of determination'] > best_tensor['coefficient of determination']:
            best_tensor = tensor
    return best_tensor

def iterate_regression():
    # Read solvers.json
    with open(DIR + 'solvers.json', 'r') as file:
        solvers = json.load(file)

    tensors = {}

    # Make regression for each solver.
    for solver in solvers:
        if solvers[solver]=={}: continue
        tensor = solver_regression(solver=solvers[solver])

        print('SOLVER --> ', solver)
        print('R2 --> ', tensor['coefficient of determination'])
        print('TENSOR --> ', tensor['tensor params'])
        print(' ------ ')

        tensors.update({solver: tensor['tensor params']})

    # Write tensors.json
    with open((DIR)+'tensors.json', 'w') as file:
        json.dump(tensors, file)

if __name__ == "__main__":
    while True:
        iterate_regression()
        sleep(9999)