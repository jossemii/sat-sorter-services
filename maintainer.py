from threading import get_ident
from time import sleep
import _solve, regresion

def maintainer(ENVS: dict, LOGGER):
    _regresion = regresion.Session(ENVS=ENVS)
    _solver = _solve.Session(ENVS=ENVS)
    LOGGER('MAINTEANCE THREAD IS ' + str(get_ident()))
    while True:
        sleep(ENVS['MAINTENANCE_SLEEP_TIME'])
        _solver.maintenance()
        _regresion.maintenance()