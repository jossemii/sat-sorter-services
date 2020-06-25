import requests
import json

def new_session(gateway_uri, gateway_uri_delete):
    return Session(gateway_uri, gateway_uri_delete)

def add_solver(image):
    # Si añades uno que ya esta, se reescribirá.
    with_new_sovler = json.load(open('solvers.json','r'))
    with_new_sovler.update({image:{
        "score":0
    }})
    with open('solvers.json','w') as file:
        file.write( json.dumps( with_new_sovler, indent=4, sort_keys=True) )

class Session:
    def __init__(self, gateway_uri, gateway_uri_delete):
        self.auth = self.make_auth()
        self.gateway_uri = gateway_uri
        self.gateway_uri_delete  = gateway_uri_delete
        self.dont_stop = True
        self.solvers = self.load_solvers()
        self.uris = self.make_uris()
        self.start()

    def get_auth(self):
        return self.auth
    
    def make_auth(self):
        import random
        import string
        def randomString(stringLength=8):
            letters = string.ascii_lowercase
            return ''.join(random.choice(letters) for i in range(stringLength))
        return randomString()

    def load_solvers(self):
        return json.load(open('solvers.json','r'))

    def get_image_uri(self, image):
        response = requests.get(self.gateway_uri+image)
        return response.text

    def make_uris(self):
        uris = {}
        for solver in self.solvers:
            uris.update({solver:self.get_image_uri(solver)})
        return uris

    def random_cnf(self):
        random_uri = self.get_image_uri('a05d3653fc494b92ea64c1308771cf8ae154f663e9156a3914e369d4501d6913')
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

    def stop(self):
        self.dont_stop == False
        with open('solvers.json', 'w') as file:
            file.write( json.dumps(self.solvers, indent=4, sort_keys=True) )   

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

        # En caso de fallo el tiempo tardado se resta al score.
        while self.dont_stop:
            cnf = self.random_cnf()
            is_insat = True # En caso en que se demuestre lo contrario.
            insats = {} # Solvers que afirman la insatisfactibilidad junto con su respectivo tiempo.
            for solver in self.solvers:    
                interpretation, time = requests.post( self.uris.get(solver)+'/', json={'cnf':cnf} ).json().get('interpretation')
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