from singleton import Singleton
import threading
from time import sleep
import numpy as np
from sklearn.linear_model import LinearRegression
from sklearn.preprocessing import PolynomialFeatures
from skl2onnx import convert_sklearn
from skl2onnx.common.data_types import Int64TensorType
import api_pb2, solvers_dataset_pb2
from threading import Thread, get_ident
from start import DIR, LOGGER, SHA3_256

# Update the tensor with the dataset.
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

def iterate_regression(TENSOR_SPECIFICATION, MAX_DEGREE, data_set) -> api_pb2.onnx__pb2.ONNX:
    LOGGER('ITERATING REGRESSION')
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

    return onnx

class Session(metaclass=Singleton):

    def __init__(self, ENVS) -> None:
        self.lock = threading.Lock()
        self.data_set = solvers_dataset_pb2.DataSet()
        self.onnx = api_pb2.onnx__pb2.ONNX()
        Thread(target=self.init, name='Regression', args=(ENVS,)).start()

    # Add new data
    def add_data(self, new_data_set: solvers_dataset_pb2.DataSet) -> None:
        self.lock.acquire()
        for hash, solver_data in new_data_set.data.items():
            if hash in self.data:
                for cnf, data in solver_data.data:
                    if cnf in self.data[hash].data:
                        self.data[hash].data[cnf].score = sum([
                            (self.data[hash].data[cnf].index * self.data[hash].data[cnf].score),
                            data.index * data.score,
                        ]) / (self.data[hash].data[cnf].index + data.index)
                        self.data[hash].data[cnf].index = self.data[hash].data[cnf].index + data.index
                        
                    else:
                        self.data[hash].data.update({
                            cnf : data
                        })
            else:
                self.data.update({
                    hash : solver_data
                })
        self.lock.release()

    # Return the tensor for the grpc stream method.
    def get_tensor(self) -> api_pb2.onnx__pb2.ONNX:
        # No hay condiciones de carrera aunque lo reescriba en ese momento.
        return self.onnx
    
    # Hasta que se implemente AddTensor en el clasificador.
    def get_data_set(self) -> solvers_dataset_pb2.DataSet:
        return self.data_set

    def init(self, ENVS):
        # set used envs on variables.
        time_for_each_regression_loop = ENVS['TIME_FOR_EACH_REGRESSION_LOOP']
        max_degree = ENVS['MAX_REGRESSION_DEGREE']
        data_set_hash = ""

        def generate_tensor_spec():
            # Performance
            p = api_pb2.hyweb__pb2.Tensor.Index()
            p.id = "score"
            p.hashtag.tag.extend(["performance"])
            # Number clauses
            c = api_pb2.hyweb__pb2.Tensor.Index()
            c.id = "clauses"
            c.hashtag.tag.extend(["number of clauses"])
            # Number of literals
            l = api_pb2.hyweb__pb2.Tensor.Index()
            l.id = "literals"
            l.hashtag.tag.extend(["number of literals"])
            # Solver services
            s = api_pb2.hyweb__pb2.Tensor.Index()
            s.id = "solver"
            s.hashtag.tag.extend(["SATsolver"])
            with open(DIR + '.service/solver.field', 'rb') as f:
                s.field.ParseFromString(f.read())

            tensor_specification = api_pb2.hyweb__pb2.Tensor()
            tensor_specification.index.extend([c, l, s, p])
            tensor_specification.rank = 3
            return tensor_specification

        LOGGER('INIT REGRESSION THREAD '+ str(get_ident()))
        while True:
            sleep(time_for_each_regression_loop)

            # Obtiene una hash del dataset para saber si se han añadido datos.
            actual_hash = SHA3_256(
                value = self.data_set.SerializeToString()
                ).hex()
            LOGGER('Check if dataset was modified ' + actual_hash + data_set_hash)
            if actual_hash != data_set_hash:
                data_set_hash = actual_hash
                
                # Se evita crear condiciones de carrera.
                self.lock.acquire()
                data_set = solvers_dataset_pb2.DataSet()
                data_set.CopyFrom(self.data_set)
                self.lock.release()

                self.onnx.CopyFrom(
                    iterate_regression(
                        MAX_DEGREE=max_degree,
                        TENSOR_SPECIFICATION=generate_tensor_spec(),
                        data_set = data_set
                    )
                )