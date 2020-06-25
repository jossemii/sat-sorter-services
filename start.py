from flask import Flask, request
import gotzilla

app = Flask(__name__)

if __name__ == "__main__":

    @app.route('/start', methods=['GET', 'POST'])
    def start():
        global Session # Una sesion por instancia.
        gateway_uri=request.json.get('gateway_uri')
        gateway_uri_delete=request.json.get('gateway_uri_delete')
        Session = gotzilla.new_session(gateway_uri, gateway_uri_delete)
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