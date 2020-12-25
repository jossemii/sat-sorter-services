DIR = ''#'/satrainer/'

def get_score(tensor: dict, cnf: dict):
    return tensor['coefficient'] * (cnf['clauses']**tensor['c']) * (cnf['literals']**tensor['l'])

def data(cnf: dict):
    for clause in cnf:
        for literal in clause:
            if abs(literal) > num_literals:
                num_literals = abs(literal)
    return {'clauses': len(cnf), 'literals': num_literals}

def cnf(cnf: dict):
    with open(DIR+'tensors.json', 'r') as tensors:
        best_interpretation = None
        best_interpretation_score = 0
        for tensor in tensors:
            score = get_score(tensor=tensors[tensor], cnf=data(cnf))
            if best_interpretation_score < score:
                best_interpretation, best_interpretation_score = tensor, score
        return best_interpretation