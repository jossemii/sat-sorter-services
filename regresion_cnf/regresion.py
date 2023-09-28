from typing import Dict, Callable

import numpy as np
from skl2onnx import convert_sklearn
from skl2onnx.common.data_types import Int64TensorType
from sklearn.linear_model import LinearRegression
from sklearn.preprocessing import PolynomialFeatures

import regresion_pb2
import solvers_dataset_pb2


# Update the tensor with the dataset.
def regression_with_degree(degree: int, input: np.array, output: np.array):
    poly = PolynomialFeatures(degree=degree, include_bias=False)
    input = poly.fit_transform(input)

    # Create a model of regression.
    model = LinearRegression().fit(input, output)
    return {
        'coefficient': model.score(input, output),
        'model': model
    }


def solver_regression(solver: Dict[str, solvers_dataset_pb2.Data], MAX_DEGREE, LOGGER: Callable):
    # Get input variables. Num of cnf variables and Num of cnf clauses.
    # num_clauses : num_literals
    input = np.array(
        [[int(var) for var in cnf.split(':')] for cnf in solver]
    ).reshape(-1, 2)

    # Get output variable. Score.
    output = np.array(
        [value.score for value in solver.values()]
    ).reshape(-1, 1)
    if len(input) != len(output):
        raise Exception('Error en solvers dataset, faltan scores.')

    best_tensor = {'coefficient': 0}
    for degree in range(1, MAX_DEGREE + 1):
        LOGGER(' DEGREE --> ' + str(degree))
        tensor = regression_with_degree(degree=degree, input=input, output=output)
        LOGGER('                R2 --> ' + str(tensor['coefficient']))
        if tensor['coefficient'] > best_tensor['coefficient']:
            best_tensor = tensor

    # Convert into ONNX format
    #   In the case that best_tensor hasn't got an model means
    #    that linear regression could not perform.
    return convert_sklearn(
        best_tensor['model'],
        initial_types=[
            ('X', Int64TensorType([None, 2])),
            # we need to use None for dynamic number of inputs because of changes in latest onnxruntime.
            # The shape is , the first dimension is the number of rows followed by the number of features.
        ]
    )


def iterate_regression(
        max_degree: int,
        data_set: solvers_dataset_pb2.DataSet,
        log: Callable
) -> regresion_pb2.Tensor:
    log('ITERATING REGRESSION')
    onnx = regresion_pb2.Tensor()

    # Make regression for each solver.
    for instance in data_set.data:
        solver_config_id, solver_data = instance.configuration_hash, instance.data
        # Si hemos tomado menos de cinco ejemplos, podemos esperar a la siguiente iteración.
        if len(solver_data) < 5:
            break
        log('SOLVER --> ' + str(solver_config_id))
        # ONNXTensor
        tensor = regresion_pb2.Tensor.NonEscalarDimension.NonEscalar()
        tensor.element = solver_config_id

        # We need to serialize and parse the buffer because the classes are provided by different proto files.
        #  It is because import the onnx-ml.proto on skl2onnx lib to our onnx.proto was impossible.
        #  The CopyFrom method checks the package name, so it thinks that are different messages. But we know
        #  that it's not true.
        tensor.escalar.ParseFromString(
            solver_regression(
                solver=solver_data,
                MAX_DEGREE=max_degree,
                LOGGER=log
            ).SerializeToString()
        )
        onnx.non_escalar.non_escalar.append(tensor)
        log(' ****** ')

    return onnx
