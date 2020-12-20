def cnf(cnf):
    with open('/satrainer/solvers.json', 'r') as solvers:
        best_interpretation = None
        best_interpretation_score = 0
        for solver in solvers:
            if cnf in solver and best_interpretation_score<solver[cnf]['score']:
                best_interpretation, best_interpretation_score = solver, solver[cnf]['score']
        return best_interpretation