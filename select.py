def select(cnf):
    solvers = open('solvers.json', 'r')
    best_interpretation, best_interpretation_score = None, 0
    for solver in solvers:
        if cnf in solver and best_interpretation_score<solver[cnf]['score']:
            best_interpretation, best_interpretation_score = solver, solver[cnf]['score']
    return best_interpretation
