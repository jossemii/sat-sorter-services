from threading import get_ident
from time import sleep
import _solve, regresion

def maintainer(ENVS: dict, LOGGER):
    _regresion = regresion.Session(ENVS=ENVS)
    _solver = _solve.Session(ENVS=ENVS)
    LOGGER('MAINTEANCE THREAD IS ' + str(get_ident()))
    load = 0
    while True:
        sleep(ENVS['MAINTENANCE_SLEEP_TIME'])
        _solver.maintenance()
        if load == int(ENVS['TIME_FOR_EACH_REGRESSION_LOOP'] / ENVS['MAINTENANCE_SLEEP_TIME']):
            try:
                _regresion.maintenance()
                load = 0
            except: continue # Mientras falle sigue intentandolo cada Maintenance sleep time.
        else:
            load += 1