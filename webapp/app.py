import sys
sys.path.insert(0,'..')

import numpy as np

from collections import defaultdict

from flask_socketio import SocketIO
from flask import Flask, render_template, make_response, request, Response

from pymongo import MongoClient

import json

import configparser

from search import SearchEngine

app = Flask('Finding friends app')
client = MongoClient()
db = client.ir_project

eng = SearchEngine(db)


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


def get_topics(ids):
    return {u['uid']:u['topics'] for u in db.topics.find({"uid": {"$in": ids}})}


def get_users(text, filters, count=30):
    search_res = {uid:score for uid,score in eng.search('BM25', 'search', text, 20, 1, (18, 25), 1)}

    res = [u for u in db.users.find({'uid': {'$in': list(search_res.keys())}})]

    photos = defaultdict(lambda: 'https://vk.com/images/camera_200.png',
                            {u['uid']:u['photo_max_orig'] for u in db.user_info.find({"uid": {"$in": [r['uid'] for r in res]}})}) 
    topics = defaultdict(lambda: np.zeros(25).tolist(), get_topics([r['uid'] for r in res]))


    for r in res:
        r['photo'] = photos[r['uid']]
        # r['sex'] = gender_map[r['sex']]
        # r['city'] = city_map[r['city']]
        r['sex'] = 'Female'
        r['city'] = 'Saint-Petersburg'
        r['topics'] = topics[r['uid']]
        r['score'] = search_res[r['uid']]

        del r['_id']

    return sorted(res, key=lambda r: r['score'], reverse=False)


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