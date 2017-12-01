import numpy as np

from collections import defaultdict

from flask_socketio import SocketIO
from flask import Flask, render_template, make_response, request, Response

from pymongo import MongoClient

import json

import configparser

app = Flask('Finding friends app')
client = MongoClient()
db = client.ir_project


city_map = defaultdict(lambda: 'Unknown',
                        {   
                            1: 'Saint-Petersburg',
                            2: 'Moscow',
                            169: 'Yaroslavl'
                        })

gender_map = defaultdict(lambda: 'Unknown',
                        {   
                            1: 'Female',
                            2: 'Male',
                        })


@app.route('/')
def hello_world():
    return render_template('index.html')


def get_topics(uid):
    topics = np.random.uniform(size=25)
    topics /= np.sum(topics)

    return topics.tolist()


def get_users(text, filters, count=30):
    res = [{k:val for k, val in u.items() if k != '_id'} for u in db.users.aggregate([{'$match':{'sex':1}}, {'$sample': {'size':count}}])]

    photos = defaultdict(lambda: 'https://vk.com/images/camera_200.png',
                            {u['uid']:u['photo_max_orig'] for u in db.user_info.find({"uid": {"$in": [r['uid'] for r in res]}})}) 

    for r in res:
        r['photo'] = photos[r['uid']]
        r['sex'] = gender_map[r['sex']]
        r['city'] = city_map[r['city']]
        r['topics'] = get_topics(r['uid'])
        r['score'] = round(np.random.uniform(), 3)

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