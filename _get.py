import onnx_pb2, solvers_dataset_pb2, api_pb2
import onnxruntime as rt
from start import DIR, LOGGER

def get_score(model: onnx_pb2.ModelProto, _cnf: dict) -> float:
    session = rt.InferenceSession(model.SerializeToString())
    inname = [input.name for input in session.get_inputs()]
    outname = [output.name for output in session.get_outputs()]

    LOGGER("inputs name:"+str(inname)+"|| outputs name:"+str(outname))
    return session.run(outname, {inname[0]: _cnf})

def data(cnf: api_pb2.Cnf) -> dict:
    num_literals = 0
    for clause in cnf.clause:
        for literal in clause.literal:
            if abs(literal) > num_literals:
                num_literals = abs(literal)
    return {'clauses': len(cnf), 'literals': num_literals}

def cnf(cnf: api_pb2.Cnf) -> solvers_dataset_pb2.SolverWithConfig:
    with open(DIR+'tensor.onnx', 'rb') as file:
        tensors = onnx_pb2.ONNX()
        tensors.ParseFromString(file.read())

        best_interpretation = None
        best_interpretation_score = 0
        for tensor in tensors.tensor:
            score = get_score(model=tensor.model, _cnf=data(cnf=cnf))
            if best_interpretation_score < score:
                best_interpretation, best_interpretation_score = tensor.element, score
        return best_interpretation