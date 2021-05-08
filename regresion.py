from time import sleep
import numpy as np
from sklearn.linear_model import LinearRegression
from sklearn.preprocessing import PolynomialFeatures
from skl2onnx import convert_sklearn
from skl2onnx.common.data_types import Int64TensorType
import api_pb2, solvers_dataset_pb2
from threading import get_ident
from start import DIR, LOGGER

def regression_with_degree(degree: int, input: np.array, output: np.array):
    poly = PolynomialFeatures(degree= degree, include_bias=False)
    input = poly.fit_transform(input)

    # Create a model of regression.
    model = LinearRegression().fit(input, output)
    return {
        'coefficient': model.score(input, output),
        'model': model
    }

def solver_regression(solver: dict, MAX_DEGREE):
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
        tensor = regression_with_degree(degree=degree, input=input, output=output)
        LOGGER('                R2 --> ' + str(tensor['coefficient']))
        if tensor['coefficient'] > best_tensor['coefficient']:
            best_tensor = tensor
    
    # Convert into ONNX format
    return convert_sklearn(
        best_tensor['model'], 
        initial_types=[
            ('X', Int64TensorType([None, 4])), # we need to use None for dynamic number of inputs because of changes in latest onnxruntime.
                                                      # The shape is , the first dimension is the number of rows followed by the number of features.
        ]
    )

def iterate_regression(TENSOR_SPECIFICATION, MAX_DEGREE):
    # Read solvers dataset
    with open(DIR + 'solvers_dataset.bin', 'rb') as file:
        data_set = solvers_dataset_pb2.DataSet()
        data_set.ParseFromString(file.read())

    onnx = api_pb2.onnx__pb2.ONNX()
    onnx.specification.CopyFrom(TENSOR_SPECIFICATION)

    # Make regression for each solver.
    for s in data_set.data:
        LOGGER('SOLVER --> ' + str(s.solver.definition))
        # ONNXTensor
        tensor = api_pb2.onnx__pb2.ONNX.ONNXTensor()
        tensor.element.value = s.solver.SerializeToString()
        # We need to serialize and parse the buffer because the clases are provided by different proto files.
        #  It is because import the onnx-ml.proto on skl2onnx lib to our onnx.proto was impossible.
        #  The CopyFrom method checks the package name, so it thinks that are different messages. But we know
        #  that it's not true.
        tensor.model.ParseFromString(
            solver_regression(solver=dict(s.data), MAX_DEGREE=MAX_DEGREE).SerializeToString()
            )
        onnx.tensor.append( tensor )
        LOGGER(' ****** ')

    # Write tensors
    with open(DIR+'tensor.onnx', 'wb') as file:
        file.write(onnx.SerializeToString())

def init(ENVS):

    # set used envs on variables.
    TIME_FOR_EACH_REGRESSION_LOOP = ENVS['TIME_FOR_EACH_REGRESSION_LOOP']
    MAX_DEGREE = ENVS['MAX_REGRESSION_DEGREE']

    def generate_tensor_spec():
        # Performance
        p = api_pb2.ipss__pb2.Tensor.Index()
        p.id = "p"
        p.tag.extend(["performance"])
        # Number clauses
        c = api_pb2.ipss__pb2.Tensor.Index()
        c.id = "c"
        c.tag.extend(["number of clauses"])
        # Number of literals
        l = api_pb2.ipss__pb2.Tensor.Index()
        l.id = "l"
        l.tag.extend(["number of literals"])
        # Solver services
        s = api_pb2.ipss__pb2.Tensor.Index()
        s.id = "s"
        s.tag.extend(["SATsolver"])
        with open(DIR + '.service/s.field', 'rb') as file:
            s.field.ParseFromString(file.read())

        TENSOR_SPECIFICATION = api_pb2.ipss__pb2.Tensor()
        TENSOR_SPECIFICATION.index.extend([c, l, s, p])
        TENSOR_SPECIFICATION.rank = 3
        return TENSOR_SPECIFICATION

    LOGGER('INIT REGRESSION THREAD '+ str(get_ident()))
    while True:
        sleep(TIME_FOR_EACH_REGRESSION_LOOP)
        iterate_regression(
            MAX_DEGREE=MAX_DEGREE, 
            TENSOR_SPECIFICATION=generate_tensor_spec()
        )