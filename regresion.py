from time import sleep

import numpy as np
from sklearn.linear_model import LinearRegression
from sklearn.preprocessing import PolynomialFeatures

import json
DIR = '' # /satrainer/

def solver_regresion(solver: dict, coffdet: tuple):

    # Get input variables. Num of cnf variables and Num of cnf clauses.
    input = np.array(
        [[int(var) for var in value.split(':')] for value in solver]
    ).reshape(-1, 2)

    # Get output variable. Score.
    output = np.array(
        [value.get('score') or None for value in solver.values()]
    ).reshape(-1, 1)

    # Utility to transform the input into a 'degree' grade function.
    input = PolynomialFeatures(degree=2, include_bias=True).fit_transform(input)

    # Create a model of regression.
    while r2 not in range(coffdet[0], coffdet[1]):
        model = LinearRegression(fit_intercept=False).fit(input, output)
        r2 = model.score() # coefficient of determination

    return [model.intercept_]+model.coef_

def iterate_regresion(coffdet: tuple):
    # Read solvers.json
    with open(DIR + 'solvers.json', 'r') as file:
        solvers = json.load(file)

    # Make regression for each solver.
    for solver in solvers:
        solver_regresion(solver=solvers[solver], coffdet=coffdet)

if __name__ == "__main__":
    while True:
        iterate_regresion((0.5, 0.8))
        sleep(20)

