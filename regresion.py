from time import sleep
import numpy as np
from sklearn.linear_model import LinearRegression
from sklearn.preprocessing import PolynomialFeatures
import json
from start import DIR, LOGGER
from start import MAX_REGRESSION_DEGREE as MAX_DEGREE


def regression_with_degree(degree: int, input: np.array, output: np.array):
    poly = PolynomialFeatures(degree= degree, include_bias=False)
    input = poly.fit_transform(input)

    # Create a model of regression.
    model = LinearRegression().fit(input, output)
    return {
        'tensor coefficients': model.intercept_.tolist()+model.coef_[0].tolist(),
        'coefficient of determination': model.score(input, output),
        'feature names': poly.get_feature_names(['c', 'l'])
    }

def solver_regression(solver: dict):
    # Get input variables. Num of cnf variables and Num of cnf clauses.
    input = np.array(
        [[int(var) for var in value.split(':')] for value in solver]
    ).reshape(-1, 2)

    # Get output variable. Score.
    output = np.array(
        [value['score'] for value in solver.values() if type(value.get('score') or None) is float]
    ).reshape(-1, 1)

    if len(input) != len(output):
        raise Exception('Error en solvers.json, faltan scores.')

    best_tensor = {'coefficient of determination': 0}
    for degree in range(1, MAX_DEGREE+1):
        LOGGER(' DEGREE --> ' + str(degree))
        tensor = regression_with_degree(degree= degree, input=input, output=output)
        LOGGER('                R2 --> ' + str(tensor['coefficient of determination']))
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
                c_exp = 1 if feature[0] == 'c' else int(feature[0][2:])
                if len(feature)==2:
                    l_exp = 1 if feature[1] == 'l' else int(feature[1][2:])
                else:
                    l_exp = 0
            else:
                l_exp = 1 if feature[0] == 'l' else int(feature[0][2:])
                c_exp = 0
            LOGGER('Feature --> ' + str(features[index-1]) + ' == ' + str(c_exp) + '--' + str(l_exp))
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
        LOGGER('SOLVER --> ' + str(solver))
        tensor = solver_regression(solver=solvers[solver])
        LOGGER(' ------ ')

        tensors.update({
            solver: into_tensor(coefficients=tensor['tensor coefficients'], features=tensor['feature names'])
            })
        LOGGER(' ****** ')

    # Write tensors.json
    with open(DIR+'tensors.json', 'w') as file:
        json.dump(tensors, file)

if __name__ == "__main__":
    while True:
        iterate_regression()
        sleep(9999)