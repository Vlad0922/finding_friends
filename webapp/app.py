from flask_socketio import SocketIO
from flask import Flask, render_template, make_response, request

import configparser

app = Flask('Finding friends app')

@app.route('/')
def hello_world():
    return render_template('index.html', data='test')


def start_app(config):
    port = config['SERVER_INFO'].getint('PORT')
    ip = config['SERVER_INFO'].get('IP')
    
    socketio = SocketIO(app)
    socketio.run(app)

if __name__ == '__main__':
    config = configparser.ConfigParser()
    config.read('config.ini')

    start_app(config)