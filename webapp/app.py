import numpy as np

from flask_socketio import SocketIO
from flask import Flask, render_template, make_response, request, Response

from pymongo import MongoClient

import json

import configparser

app = Flask('Finding friends app')
client = MongoClient()
db = client.ir_project

@app.route('/')
def hello_world():
    return render_template('index.html')


def get_topics(uid):
	topics = np.random.uniform(size=25)
	topics /= np.sum(topics)

	return topics.tolist()

def get_users(text, filters, count=30):
    res = [{k:val for k, val in u.items() if k != '_id'} for u in db.users.aggregate([{'$sample': {'size':count}}])]

    for r in res:
        r['topics'] = get_topics(r['uid'])

    return res


@app.route('/process_query', methods=['POST', 'GET'])
def process_query():
    q = request.args.get("text")
    f = request.args

    res = get_users(q, f)

    return json.dumps(res)


def start_app(config):
    port = config['SERVER_INFO'].getint('PORT')
    ip = config['SERVER_INFO'].get('IP')
    
    socketio = SocketIO(app)
    socketio.run(app)

if __name__ == '__main__':
    config = configparser.ConfigParser()
    config.read('config.ini')

    start_app(config)