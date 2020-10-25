import requests
import json

class Singleton(type):
    _instances = {}
    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]

class Session(metaclass=Singleton):

    def stop(self):
        self.working = False

    def load_solver( self, solver):
        self.solvers.update({solver:{}})
        self.uris.update({solver:self.get_image_uri(solver)})

    def get_image_uri(self, image):
        while True:
            try:
                response = requests.get('http://'+self.gateway + '/' + image, timeout=30)
            except requests.HTTPError as e:
                print('Error al solicitar solver, ', image, e)
                pass
            if response and response.status_code == 200:
                return response.json()

    def init_random_cnf_service(self):
        try:
            random_dict = self.get_image_uri('3d67d9ded8d0abe00bdaa9a3ae83d552351afebe617f4e7b0653b5d49eb4e67a')
            if random_dict and random_dict.status_code == 200:
                self.random_uri = random_dict.get('uri')
                self.random_cnf_token = random_dict.get('token')
            else: raise requests.HTTPError
        except requests.HTTPError as e:
            print('Error al solicitar random cnf, ', e)
            self.init_random_cnf_service()

    def random_cnf(self):
        while True:
            try:
                print('OBTENINEDO RANDON CNF')
                response = requests.get('http://'+self.random_uri+'/', timeout=30)
                print('RESPUESTA DEL CNF --> ', response, response.text)
            except (TimeoutError, requests.exceptions.ReadTimeout) or requests.exceptions.ConnectionError or requests.HTTPError:
                print('VAMOS A CAMBIAR EL SERVICIO DE OBTENCION DE CNFs RANDOM')
                requests.get('http://'+self.random_cnf_token+'/', timeout=30)
                self.init_random_cnf_service()
                print('listo. ahora vamos a probar otra vez.')
            if response and response.status_code == 200 and 'cnf' in response.json():
                return response.json().get('cnf')

    @staticmethod
    def isGood(cnf, interpretation):
        def goodClause(clause, interpretation):
            for var in clause:
                for i in interpretation:
                    print('      ', var, i)
                    if var == i:
                        return True
            return False
        interpretation = interpretation.split(' ')[1:]
        cnf = [clause.split(' ')[:-1] for clause in cnf.split('\n')[2:-1]]
        for clause in cnf:
            if goodClause(clause, interpretation) == False:
                return False
        return True

    def updateScore( self, cnf, solver, score):
        num_clauses, num_literals = (
            cnf.split('\n')[1].split(' ')[-2],
            cnf.split('\n')[1].split(' ')[-1],
        )
        try:
            solver_cnf = self.solvers[solver][str(num_clauses)+':'+str(num_literals)]
        except Exception:
            solver_cnf = {
                'index': 1,
                'score': 0
            }
        self.solvers[solver].update({
                str(num_clauses)+':'+str(num_literals) : {
                    'index': solver_cnf['index']+1,
                    'score': ( solver_cnf['score']*solver_cnf['index'] + score )/(solver_cnf['index']+1)
                }
            })

    def init(self, gateway, refresh=2):

        self.working = True
        self.refresh = int(refresh)
        self.gateway = gateway
        self.solvers = json.load(open('/satrainer/solvers.json','r'))
        print('OBTENIENDO IMAGENES..')
        self.uris = { solver : self.get_image_uri(solver) for solver in self.solvers }
        print('listo')
        refresh = 0
        timeout=30
        print('INICIANDO SERVICIO DE RANDOM CNF')
        self.init_random_cnf_service()
        print('hecho.')
        while self.working:
            if refresh < self.refresh:
                print('REFRESH ES MENOR')
                refresh = refresh+1
                cnf = self.random_cnf()
                is_insat = True # En caso en que se demuestre lo contrario.
                insats = {} # Solvers que afirman la insatisfactibilidad junto con su respectivo tiempo.
                print('VAMOS A PROBAR LOS SOLVERS')
                for solver in self.solvers:
                    print('SOVLER --> ', solver)
                    try:
                        # El timeout se podria calcular a partir del resto ...
                        # Tambien podria ser asincrono ...
                        print('RESPUESTA DEL SOLVER -->')
                        response = requests.post(
                            'http://'+ self.uris.get(solver).get('uri')+'/',
                            json={'cnf':cnf}, timeout=timeout
                        )
                        print(response.text)
                        interpretation = response.json().get('interpretation')
                        time = int(response.elapsed.total_seconds())
                        if interpretation == '':
                            insats.update({solver:time})
                        else:
                            if self.isGood(cnf, interpretation):
                                is_insat = False
                            else:
                                pass
                            if time==0:score=+1
                            else: score=float(-1/time)
                            self.updateScore(
                                cnf = cnf,
                                solver = solver,
                                score = score
                            )
                    except (TimeoutError, requests.exceptions.ReadTimeout):
                        print('TIME OUT NO SUPERADO.')
                        if timeout==0:score=-1
                        else: score=float(-1/timeout)
                        self.updateScore(
                            cnf = cnf,
                            solver = solver,
                            score = score
                        )

                # Registra los solvers que afirmaron la insatisfactibilidad en caso en que ninguno
                #  haya demostrado lo contrario.
                if is_insat:
                    for solver in insats:
                        self.updateScore(
                            cnf = cnf,
                            solver = solver,
                            score =  float(+1/insats.get(solver))
                        )
                else:
                    for solver in insats:
                        self.updateScore(
                            cnf = cnf,
                            solver = solver,
                            score = float(-1/insats.get(solver))
                        )
            else:
                print('ACTUALIZA EL JSON')
                refresh = 0
                with open('/satrainer/solvers.json', 'w') as file:
                    json.dump(self.solvers, file)
