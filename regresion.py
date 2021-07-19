import hashlib
from time import sleep
import numpy as np
from sklearn.linear_model import LinearRegression
from sklearn.preprocessing import PolynomialFeatures
from skl2onnx import convert_sklearn
from skl2onnx.common.data_types import Int64TensorType, FloatTensorType
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
    # num_clauses : num_literals
    input = np.array(
        [[int(var) for var in value.split(':')] for value in solver]
    ).reshape(-1, 2)

    # Get output variable. Score.
    output = np.array(
        [value.score for value in solver.values()]
    ).reshape(-1, 1)
    if len(input) != len(output):
        raise Exception('Error en solvers dataset, faltan scores.')

    best_tensor = {'coefficient': 0}
    for degree in range(1, MAX_DEGREE+1):
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
            ('X', Int64TensorType([None, 2])), # we need to use None for dynamic number of inputs because of changes in latest onnxruntime.
                                                # The shape is , the first dimension is the number of rows followed by the number of features.
        ]
    )

def iterate_regression(TENSOR_SPECIFICATION, MAX_DEGREE, data_set):
    onnx = api_pb2.onnx__pb2.ONNX()
    onnx.specification.CopyFrom(TENSOR_SPECIFICATION)

    # Make regression for each solver.
    for s in data_set.data:
        # Si hemos tomado menos de cinco ejemplos, podemos esperar a la siguiente iteración.
        if len(s.data) < 5: break
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
    time_for_each_regression_loop = ENVS['TIME_FOR_EACH_REGRESSION_LOOP']
    max_degree = ENVS['MAX_REGRESSION_DEGREE']
    data_set_hash = ""

    def generate_tensor_spec():
        # Performance
        p = api_pb2.ipss__pb2.Tensor.Index()
        p.id = "score"
        p.tag.extend(["performance"])
        # Number clauses
        c = api_pb2.ipss__pb2.Tensor.Index()
        c.id = "clauses"
        c.tag.extend(["number of clauses"])
        # Number of literals
        l = api_pb2.ipss__pb2.Tensor.Index()
        l.id = "literals"
        l.tag.extend(["number of literals"])
        # Solver services
        s = api_pb2.ipss__pb2.Tensor.Index()
        s.id = "solver"
        s.tag.extend(["SATsolver"])
        with open(DIR + '.service/solver.field', 'rb') as f:
            s.field.ParseFromString(f.read())

        tensor_specification = api_pb2.ipss__pb2.Tensor()
        tensor_specification.index.extend([c, l, s, p])
        tensor_specification.rank = 3
        return tensor_specification

    LOGGER('INIT REGRESSION THREAD '+ str(get_ident()))
    while True:
        sleep(time_for_each_regression_loop)

        # Read solvers dataset
        with open(DIR + 'solvers_dataset.bin', 'rb') as file:
            data_set = solvers_dataset_pb2.DataSet()
            data_set.ParseFromString(file.read())

        # Obtiene una hash del dataset para saber si se han añadido datos.
        actual_hash = hashlib.sha3_256(data_set.ParseFromString()).hexdigest()
        if actual_hash != data_set_hash:
            iterate_regression(
                MAX_DEGREE=max_degree,
                TENSOR_SPECIFICATION=generate_tensor_spec(),
                data_set=data_set
            )
            data_set_hash = actual_hash