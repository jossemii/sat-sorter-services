import onnx_pb2, solvers_dataset_pb2
import onnxruntime as rt
from start import DIR

def get_score(model: onnx_pb2.ModelProto, _cnf: dict) -> float:
    session = rt.InferenceSession(model.SerializeToString())
    inname = [input.name for input in session.get_inputs()]
    outname = [output.name for output in session.get_outputs()]

    print("inputs name:",inname,"|| outputs name:",outname)
    return session.run(outname, {inname[0]: data_input})

def data(cnf: list) -> dict:
    num_literals = 0
    for clause in cnf:
        for literal in clause:
            if abs(literal) > num_literals:
                num_literals = abs(literal)
    return {'clauses': len(cnf), 'literals': num_literals}

def cnf(cnf: list) -> solvers_dataset_pb2.SolverWithConfig():
    with open(DIR+'tensor.onnx', 'rb') as file:
        tensors = onnx_pb2.ONNX()
        tensors.ParseFromString(file.read())
        data_cnf = data(cnf=cnf)

        best_interpretation = None
        best_interpretation_score = 0
        for tensor in tensors.tensor:
            score = get_score(model=tensor.model, _cnf=data_cnf)
            if best_interpretation_score < score:
                best_interpretation, best_interpretation_score = tensor.element, score
        return best_interpretation