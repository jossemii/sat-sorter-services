from time import sleep
import numpy as np
from sklearn.linear_model import LinearRegression
from sklearn.preprocessing import PolynomialFeatures
from skl2onnx import convert_sklearn
from skl2onnx.common.data_types import FloatTensorType
import performance_data_pb2, api_pb2
from threading import get_ident
from start import DIR, LOGGER, TIME_FOR_EACH_REGRESSION_LOOP
from start import MAX_REGRESSION_DEGREE as MAX_DEGREE
TENSOR_SPECIFICATION = None

def regression_with_degree(degree: int, input: np.array, output: np.array):
    poly = PolynomialFeatures(degree= degree, include_bias=False)
    input = poly.fit_transform(input)

    # Create a model of regression.
    model = LinearRegression().fit(input, output)
    return {
        'coefficient': model.score(input, output),
        'model': model
    }

def solver_regression(solver: dict):
    # Get input variables. Num of cnf variables and Num of cnf clauses.
    input = np.array(
        [[int(var) for var in value.split(':')] for value in solver]
    ).reshape(-1, 2)

    # Get output variable. Score.
    output = np.array(
        [value.score for value in solver.values()]
    ).reshape(-1, 1)
    if len(input) != len(output):
        raise Exception('Error en solvers.json, faltan scores.')

    best_tensor = {'coefficient': 0}
    for degree in range(1, MAX_DEGREE+1):
        LOGGER(' DEGREE --> ' + str(degree))
        tensor = regression_with_degree(degree= degree, input=input, output=output)
        LOGGER('                R2 --> ' + str(tensor['coefficient']))
        if tensor['coefficient'] > best_tensor['coefficient']:
            best_tensor = tensor
    
    # Convert into ONNX format
    return convert_sklearn(best_tensor['model'])

def iterate_regression():
    # Read solvers dataset
    with open(DIR + 'solvers_dataset.bin', 'rb') as file:
        data_set = performance_data_pb2.SolversPerformance()

    onnx = api_pb2.onnx__pb2.ONNX()
    onnx.specification.CopyFrom(TENSOR_SPECIFICATION)

    # Make regression for each solver.
    for s in data_set.data:
        LOGGER('SOLVER --> ' + str(s.solver.definition))
        # ONNXTensor
        tensor = api_pb2.onnx__pb2.ONNX.ONNXTensor()
        tensor.element.CopyFrom(s.solver)
        tensor.model.CopyFrom(solver_regression(solver=dict(s.data)))
        onnx.tensor.append( tensor )
        LOGGER(' ****** ')

    # Write tensors
    with open(DIR+'tensor.onnx', 'wb') as file:
        file.write(onnx.SerializeToString())

def init():
    def generate_tensor_spec():
        # Performance
        p = api_pb2.ipss__pb2.Tensor.Variable()
        p.id = "p"
        p.tag.extend(["performance"])
        # Number clauses
        c = api_pb2.ipss__pb2.Tensor.Variable()
        c.id = "c"
        c.tag.extend(["number of clauses"])
        # Number of literals
        l = api_pb2.ipss__pb2.Tensor.Variable()
        l.id = "l"
        l.tag.extend(["number of literals"])
        # Solver services
        s = api_pb2.ipss__pb2.Service()
        s.id = "s"
        s.tag.extend(["SATsolver"])
        with open(DIR + '.service/s.desc', 'rb') as file:
            s.field.ParseFromString(file.read())

        TENSOR_SPECIFICATION = api_pb2.ipss__pb2.Tensor()
        TENSOR_SPECIFICATION.output_variable.append(p)
        TENSOR_SPECIFICATION.input_variable.extend([c, l, s])

    LOGGER('INIT REGRESSION THREAD '+ str(get_ident()))
    generate_tensor_spec()
    while True:
        sleep(TIME_FOR_EACH_REGRESSION_LOOP)
        iterate_regression()