from flask import Flask, request
import gotzilla

app = Flask(__name__)

if __name__ == "__main__":
    
    # Una sesion por instancia.
    
    @app.route('/start', methods=['GET', 'POST'])
    def start():
        global Session 
        gateway=request.json.get('gateway')
        Session = gotzilla.new_session(gateway)
        return {'auth':Session.get_auth()}

    @app.route('/stop', methods=['POST'])
    def stop():
        global Session
        auth = request.json.get('auth')
        if auth == Session.get_auth() :
            Session.stop()
            
    @app.route('/add/<image>', methods=['POST'])
    def add(image):
        gotzilla.add_solver(image)

    app.run(host='0.0.0.0', port=8000)