import requests
import json

def start(gateway, refresh):
    Session(gateway=gateway, refresh=refresh)

class Session:
    def __init__(self, gateway, refresh):
        self.refresh = int(refresh)
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
        print('\n\n\nConecta con gateway\n'+self.gateway + '/'+ image+'\n\n')
        response = requests.get(self.gateway + '/' + image)
        return response.json()

    def make_uris(self):
        uris = {}
        for solver in self.solvers:
            uris.update({solver:self.get_image_uri(solver)})
        return uris

    def init_random_cnf(self):
        random_dict = self.get_image_uri('3d67d9ded8d0abe00bdaa9a3ae83d552351afebe617f4e7b0653b5d49eb4e67a')
        print('Iniciamos cnf random.')
        self.random_uri = random_dict.get('uri')
        self.random_cnf_token = random_dict.get('token')

    def random_cnf(self):
        print('Obtenemos cnf random.')
        while 1:
            try:
                response = requests.get(self.random_uri+'/')
                if response.status_code != 200:
                    print("Algo va mal ....", response)
                    exit()
                break
            except requests.exceptions.ConnectionError:
                print(self.random_uri+'   Docker va muy lento.....\n\n')
        cnf = response.json().get('cnf')
        print(cnf)
        return cnf

    def start(self):
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
            print('cnf ---> ',cnf,'/n/ninterpretation ---> ',interpretation)
            for clause in cnf:
                print('   ',clause)
                if goodClause(clause, interpretation) == False:
                    print('         is False')
                    return False
            print('         is True')
            return True

        refresh = 0
        timeout=30
        self.init_random_cnf()
        while 1:
            if refresh<self.refresh:
                print(refresh,' / ',self.refresh)
                refresh = refresh+1
                cnf = self.random_cnf()
                is_insat = True # En caso en que se demuestre lo contrario.
                insats = {} # Solvers que afirman la insatisfactibilidad junto con su respectivo tiempo.
                for solver in self.solvers:
                    print(solver)
                    try:
                        # El timeout se podria calcular a partir del resto ...
                        # Tambien podria ser asincrono ...
                        response = requests.post( self.uris.get(solver).get('uri')+'/', json={'cnf':cnf}, timeout=timeout )
                        interpretation = response.json().get('interpretation')
                        time = response.elapsed.total_seconds()
                        if interpretation == '':
                            print('Me dices que es insatisfactible, se guarda cada solver con el tiempo tardado.') 
                            insats.update({solver:time})
                        else:
                            print('Dice que es satisfactible .....')
                            if isGood(cnf, interpretation):
                                print('La interpretacion es correcta.') 
                                is_insat = False
                            else:
                                print('La interpretacion es incorrecta.')
                                time = -1*time
                            score = self.solvers.get(solver).get('score')+1/time
                            self.solvers.update({solver:{'score':score}})
                    except (TimeoutError, requests.exceptions.ReadTimeout):
                        print('Tradó demasiado....')
                        score = self.solvers.get(solver).get('score')-1/timeout
                        self.solvers.update({solver:{'score':score}})

                # Registra los solvers que afirmaron la insatisfactibilidad en caso en que ninguno
                #  haya demostrado lo contrario.
                if is_insat:
                    print('Estaban en lo cierto', insats)
                    for solver in insats:
                        score = self.solvers.get(solver).get('score')+1/insats.get(solver)
                        self.solvers.update({solver:{'score':score}})
                else:
                    print('Se equivocaron..')
                    for solver in insats:
                        score = self.solvers.get(solver).get('score')-1/insats.get(solver)
                        self.solvers.update({solver:{'score':score}})
            else:
                refresh = 0
                print('Actualizo el tensor.')
                solvers = self.load_solvers()
                for solver in self.solvers:
                    d = self.solvers[solver]
                    d.update({'score': self.solvers[solver].get('score') + solvers[solver].get('score')})
                    solvers.update({solver:d})
                with open('solvers.json', 'w') as file:
                    file.write( json.dumps(solvers, indent=4, sort_keys=True) )

                self.solvers_init_score()