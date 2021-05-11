import onnx_pb2, solvers_dataset_pb2, api_pb2
import onnxruntime as rt
import numpy
from start import DIR, LOGGER

def get_score(model: onnx_pb2.ModelProto, _cnf: dict) -> float:
    session = rt.InferenceSession(
        model.SerializeToString()
    )
    input_name = session.get_inputs()[0].name
    label_name = session.get_outputs()[0].name
    return session.run([label_name], {input_name: [_cnf]})[0][0][0]

def data(cnf: api_pb2.Cnf) -> dict:
    num_literals = 0
    for clause in cnf.clause:
        for literal in clause.literal:
            if abs(literal) > num_literals:
                num_literals = abs(literal)
    return [
        numpy.array((len(cnf.clause))).astype(numpy.int64),
        numpy.array((num_literals)).astype(numpy.int64)
    ]

def cnf(cnf: api_pb2.Cnf) -> solvers_dataset_pb2.SolverWithConfig:
    with open(DIR+'tensor.onnx', 'rb') as file:
        tensors = onnx_pb2.ONNX()
        tensors.ParseFromString(file.read())

        best_interpretation = solvers_dataset_pb2.SolverWithConfig()
        best_interpretation_score = 0
        for tensor in tensors.tensor:
            score = get_score(model=tensor.model, _cnf=data(cnf=cnf))
            print('SCORE IS -> ', score)
            if best_interpretation_score < score:
                best_interpretation_score = score
                best_interpretation.ParseFromString(tensor.element.value)
        print('BEST INTERPRETATION -> ', best_interpretation, 'FROM ', tensor.element.value)
        return best_interpretation