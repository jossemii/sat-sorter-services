import hashlib, logging
import json
import os.path
from typing import Dict

DEV_MODE = False
DEV_ENVS = {
    'GATEWAY_MAIN_DIR': 'localhost:8090',
    'MEM_LIMIT': 50 * pow(10, 6),
    'CLIENT_ID': 'dev'
}

ENVS = {
    'GATEWAY_MAIN_DIR': None,
    'SAVE_TRAIN_DATA': 10,
    'MAINTENANCE_SLEEP_TIME': 60,
    'SOLVER_PASS_TIMEOUT_TIMES': 5,
    'SOLVER_FAILED_ATTEMPTS': 20,
    'TRAIN_SOLVERS_TIMEOUT': 30,
    'MAX_REGRESSION_DEGREE': 100,
    'TIME_FOR_EACH_REGRESSION_LOOP': 900,
    'CONNECTION_ERRORS': 20,
    'START_AVR_TIMEOUT': 30,
    'MAX_WORKERS': 20,
    'MAX_REGRESION_WORKERS': 5,
    'MAX_DISUSE_TIME_FACTOR': 1,
    'TIME_SLEEP_WHEN_SOLVER_ERROR_OCCURS': 1,
    'MAX_ERRORS_FOR_SOLVER': 5,
}

# -- The service use sha3-256 for identify internal objects. --
SHA3_256_ID = bytes.fromhex("a7ffc6f8bf1ed76651c14756a061d662f580ff4de43b49fa82d80a4b80f8434a")
SHA3_256 = lambda value: "" if value is None else hashlib.sha3_256(value).digest()

logging.basicConfig(filename='../app.log', level=logging.DEBUG, format='%(asctime)s %(levelname)-8s %(message)s')
LOGGER = lambda message: logging.getLogger().debug(message + '\n') if not DEV_MODE else print(message + '\n')
DIR = '/satsorter/' if not DEV_MODE else ''

with open(os.path.join(DIR, "/.service/pre-compile.json")) as pre_compile:
    _js = json.load(pre_compile)

REGRESSION_SHA3_256 = _js['dependencies']['REGRESSION']
RANDOM_SHA3_256 = _js['dependencies']['RANDOM']

BLOCK_DIRECTORY = _js["blocks_directory"]
SERVICE_DIRECTORY = _js["service_dependencies_directory"]
METADATA_DIRECTORY = _js["metadata_dependencies_directory"]
CACHE_DIRECTORY = "__cache__"
