import json

DIR = ''#'/satrainer/'

def get_score(tensor: list, _cnf: dict):
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

def cnf(cnf: list):
    with open(DIR+'tensors.json', 'r') as tensors:
        tensors = json.load(tensors)
        data_cnf = data(cnf=cnf)

        best_interpretation = None
        best_interpretation_score = 0
        for tensor in tensors:
            score = get_score(tensor=tensors[tensor], _cnf=data_cnf)
            if best_interpretation_score < score:
                best_interpretation, best_interpretation_score = tensor, score
        return best_interpretation