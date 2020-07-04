import requests
import json

def start(gateway, refresh):
    Session(gateway=gateway, refresh=refresh)

class Session:
    def __init__(self, gateway, refresh):
        self.refresh = refresh
        self.gateway = gateway
        self.solvers = self.load_solvers()
        self.solvers_init_score()
        self.uris = self.make_uris()
        self.start()

    def solvers_init_score(self):
        for solver in self.solvers:
            self.solvers.update({solver:{
                'score': 0
            }})

    def load_solvers(self):
        return json.load(open('solvers.json','r'))

    def get_image_uri(self, image):
        response = requests.get(self.gateway + '/' + image)
        return response.json

    def make_uris(self):
        uris = {}
        for solver in self.solvers:
            uris.update({solver:self.get_image_uri(solver)})
        return uris

    def random_cnf(self):
        random_dict = self.get_image_uri('e7224c40ce98d3e56a60974329343be8d430031e4e87f8dd1c48f951d95f8d52')
        random_uri = random_dict.get('uri')
        self.random_cnf_token = random_dict.get('token')
        docker_snail = True
        while docker_snail==True:
            try:
                response = requests.get(random_uri+'/')
                docker_snail = False
                if response.status_code != 200:
                    print("Algo va mal ....", response)
                    exit()
            except requests.exceptions.ConnectionError:
                print('Docker va muy lento.....')
        cnf = response.json().get('cnf')
        print(cnf)
        return cnf

    def start(self):
        def isGod(cnf, interpretation):
            interpretation = interpretation.split(' ')
            cnf = [clause.split(' ') for clause in cnf.split('\n')]
            for clause in cnf:
                ok = False
                for var in clause:
                    for i in interpretation:
                        if var == i:
                            ok = True
                if ok == False:
                    return False
            return True

        refresh = 0
        while 1:
            if refresh<self.refresh:
                cnf = self.random_cnf()
                is_insat = True # En caso en que se demuestre lo contrario.
                insats = {} # Solvers que afirman la insatisfactibilidad junto con su respectivo tiempo.
                for solver in self.solvers:
                    try:
# El timeout se podria calcular a partir del resto..
                        response = requests.post( self.uris.get(solver).get('uri')+'/', json={'cnf':cnf}, timeout=30 ).json().get('interpretation')
                        interpretation = response.text
                        time = response.elapsed.total_seconds()
                        if interpretation == '':
                            # Me dices que es insatisfactible, se guarda cada solver con el tiempo tardado.
                            insats.update({solver:time})
                        else:
                            if isGod(cnf, interpretation):
                                # La interpretacion es correcta.
                                is_insat = False
                            else:
                                time = -1*time
                            score = self.solvers.get(solver)+1/time
                            self.solvers.update({solver:{'score':score}})
                    except TimeoutError:
                        # Tradó demasiado....
                        score = self.solvers.get(solver)+1/(-1*self.refresh)
                        self.solvers.update({solver:{'score':score}})

                # Registra los solvers que afirmaron la insatisfactibilidad en caso en que ninguno
                #  haya demostrado lo contrario.
                if is_insat:
                    for solver in insats:
                        score = self.solvers.get(solver)+1/insats.get(solver)
                        self.solvers.update({solver:{'score':score}})
                else:
                    for solver in insats:
                        time = -1*insats.get(solver)
                        score = self.solvers.get(solver)+1/time
                        self.solvers.update({solver:{'score':score}})
            else:
                refresh = 0
                # Actualizo el tensor.
                solvers = load_solvers()
                for solver in self.solvers:
                    d = self.solvers[solver] 
                    d.update({'score': solver:self.sovlers[solver] + solvers[solver]})
                    solvers.update({solver:d})
                with open('solvers.json', 'w') as file:
                    file.write( json.dumps(solvers, indent=4, sort_keys=True) )

                self.solvers_init_score()