import onnx_pb2, performance_data_pb2
from start import DIR

def get_score(model: onnx_pb2.ModelProto, _cnf: dict):
    score = tensor[0]['coefficient']
    for term in tensor[1:]:
        score = score + term['coefficient'] * (_cnf['clauses']**term['feature']['c']) * (_cnf['literals']**term['feature']['l'])
    return score

def data(cnf: list):
    num_literals = 0
    for clause in cnf:
        for literal in clause:
            if abs(literal) > num_literals:
                num_literals = abs(literal)
    return {'clauses': len(cnf), 'literals': num_literals}

def cnf(cnf: list) -> performance_data_pb2.Solver():
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