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
        'feature names': poly.get_feature_names(['c', 'l'])
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

def into_tensor(coefficients: np.array, features):
    if len(coefficients) != len(features)+1: raise Exception('Feature len error.')
    tensor = []
    for index in range(len(coefficients)):
        if index == 0:
            tensor.append({'coefficient': coefficients[index]})
        else:
            feature = features[index-1].split(' ')
            if feature[0][0] == 'c':
                c_exp = 1 if feature[0] == 'c' else int(feature[0][-1])
                if len(feature)==2:
                    l_exp = 1 if feature[1] == 'l' else int(feature[1][-1])
                else:
                    l_exp = 0
            else:
                l_exp = 1 if feature[0] == 'l' else int(feature[0][-1])
                c_exp = 0
            print('Feature --> ', features[index-1], ' == ', c_exp, '--' ,l_exp)
            tensor.append({
                'coefficient': coefficients[index],
                'feature': {
                    'c' : c_exp,
                    'l': l_exp
                    }
            })
    return tensor

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
        print(' ------ ')

        tensors.update({
            solver: into_tensor( coefficients=tensor['tensor coefficients'], features=tensor['feature names'])
            })

    # Write tensors.json
    with open((DIR)+'tensors.json', 'w') as file:
        json.dump(tensors, file)

if __name__ == "__main__":
    while True:
        iterate_regression()
        sleep(9999)