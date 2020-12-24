from time import sleep
import numpy as np
import pandas as pd
from sklearn.linear_model import LinearRegression
from sklearn.preprocessing import PolynomialFeatures

import json
DIR = '' # /satrainer/
MAX_DEGREE = 100


def regression_with_degree(degree: int, input: np.array, output: np.array):
    poly = PolynomialFeatures(degree= degree, include_bias=False)
    input = poly.fit_transform(input)

    # Create a model of regression.
    model = LinearRegression().fit(input, output )
    return {
        'tensor coefficients': model.intercept_.tolist()+model.coef_[0].tolist(),
        'coefficient of determination': model.score(input, output),
        'feature names': poly.get_feature_names()
    }

def solver_regression(solver: dict):
    # Get input variables. Num of cnf variables and Num of cnf clauses.
    input = pd.DataFrame(
        [[int(var) for var in value.split(':')] for value in solver]
    )

    # Get output variable. Score.
    output = np.array(
        [value.get('score') or None for value in solver.values()]
    ).reshape(-1, 1)

    best_tensor = {'coefficient of determination': 0}
    for degree in range(1, MAX_DEGREE+1):
        print(' DEGREE --> ', degree)
        tensor = regression_with_degree(degree= degree, input=input.to_numpy(), output=output)
        print('  R2 --> ', tensor['coefficient of determination'])
        if tensor['coefficient of determination'] > best_tensor['coefficient of determination']:
            best_tensor = tensor
    return best_tensor

def into_tensor(coefficients: np.array, feature_names):
    print('FEATURE NAMES --> ', feature_names)
    print('COEFFICIENTS --> ', coefficients)
    #coefficients = pd.concat([pd.DataFrame(input.columns),pd.DataFrame(np.transpose(coefficients))], axis = 1)
    """tensor = []
    for coefficient in coefficients:
        tensor.append({
            'operation': None,
            'coefficient': coefficient,
            'variables': [0, 0]
        })"""

def iterate_regression():
    # Read solvers.json
    with open(DIR + 'solvers.json', 'r') as file:
        solvers = json.load(file)

    tensors = {}

    # Make regression for each solver.
    for solver in solvers:
        if solvers[solver]=={}: continue
        print('SOLVER --> ', solver)
        tensor = solver_regression(solver=solvers[solver])
   
        print(' R2 --> ', tensor['coefficient of determination'])
        print(' TENSOR --> ', tensor['tensor coefficients'])
        print(' ------ ')

        tensors.update({
            solver: into_tensor( coefficients=tensor['tensor coefficients'], feature_names=tensor['feature names'])
            })

    # Write tensors.json
    with open((DIR)+'tensors.json', 'w') as file:
        json.dump(tensors, file)

if __name__ == "__main__":
    while True:
        iterate_regression()
        sleep(9999)