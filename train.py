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
        response = requests.get('http://'+self.gateway + '/' + image)
        return response.json()

    def init_random_cnf_service(self):
        random_dict = self.get_image_uri('3d67d9ded8d0abe00bdaa9a3ae83d552351afebe617f4e7b0653b5d49eb4e67a')
        self.random_uri = random_dict.get('uri')
        self.random_cnf_token = random_dict.get('token')

    def random_cnf(self):
        while 1:
            try:
                response = requests.get('http://'+self.random_uri+'/')
                if response.status_code != 200:
                    exit()
                break
            except requests.exceptions.ConnectionError:
                pass
        cnf = response.json().get('cnf')
        return cnf

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

    def init(self, gateway, refresh):

        self.working = True
        self.refresh = int(refresh)
        self.gateway = gateway
        self.solvers = json.load(open('/satrainer/solvers.json','r'))
        self.uris = { solver : self.get_image_uri(solver) for solver in self.solvers }
        refresh = 0
        timeout=30
        self.init_random_cnf_service()
        while self.working:
            if refresh < self.refresh:
                refresh = refresh+1
                cnf = self.random_cnf()
                is_insat = True # En caso en que se demuestre lo contrario.
                insats = {} # Solvers que afirman la insatisfactibilidad junto con su respectivo tiempo.
                for solver in self.solvers:
                    try:
                        # El timeout se podria calcular a partir del resto ...
                        # Tambien podria ser asincrono ...
                        response = requests.post('http://'+ self.uris.get(solver).get('uri')+'/', json={'cnf':cnf}, timeout=timeout )
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
                refresh = 0
                with open('/satrainer/solvers.json', 'w') as file:
                    json.dump(self.solvers, file)
