from threading import get_ident
from time import sleep
import _solve, regresion

def maintainer(ENVS: dict, LOGGER):
    _regresion = regresion.Session(ENVS=ENVS)
    _solver = _solve.Session(ENVS=ENVS)
    LOGGER('MAINTEANCE THREAD IS ' + str(get_ident()))
    load = 0    # El mantenimiento de regresion se realiza cada 10 (defect.) mantenimientos de solvers.
    while True:
        sleep(ENVS['MAINTENANCE_SLEEP_TIME'])
        _solver.maintenance()
        if load == ENVS['MAINTENANCE_TIMES_FOR_EVERY_REGRESSION_MAINTAIN']:
            load = 0
            _regresion.maintenance()
        else:
            load += 1