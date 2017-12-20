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

eng = SearchEngine()


city_map = defaultdict(lambda: 'Unknown',
                        {   
                            2: 'Saint-Petersburg',
                            1: 'Moscow',
                            169: 'Yaroslavl'
                        })

gender_map = defaultdict(lambda: 'Unknown',
                        {   
                            1: 'Female',
                            2: 'Male',
                        })


@app.route('/')
def hello_world():
    print('wow! a request for an index!')
    return render_template('index.html')


def get_topics(ids):
    return {u['uid']:u['topics'] for u in db.topics.find({"uid": {"$in": ids}})}


def topics_to_heatmap(topics):
    res = np.zeros((10, 10))
       
    for t_idx, t_val in topics:
        res[t_idx % 10, t_idx // 10] = t_val

    return res.tolist()


def get_users(text, filters, count=10):
    search_res = {int(uid):score for uid,score in eng.search('BM25', 'search', text, 10, int(filters['gender']), int(filters['city']), int(filters['age_from']), int(filters['age_to']))}

    res = [u for u in db.users.find({'uid': {'$in': list(search_res.keys())}})]

    photos = defaultdict(lambda: 'https://vk.com/images/camera_200.png',
                            {u['uid']:u['photo_max_orig'] for u in db.user_info.find({"uid": {"$in": [r['uid'] for r in res]}})}) 
    topics = defaultdict(lambda: np.zeros(25).tolist(), get_topics([r['uid'] for r in res]))

    for r in res:
        r['photo'] = photos[r['uid']]
        r['sex'] = gender_map[r['sex']]
        r['city'] = city_map[r['city']]
        #r['sex'] = 'Female'
        #r['city'] = 'Saint-Petersburg'
        r['topics_heatmap'] = topics_to_heatmap(topics[r['uid']])
        r['topics'] = topics[r['uid']]
        r['score'] = search_res[r['uid']]

        del r['_id']
    
    print(search_res)
    print(res)

    return sorted(res, key=lambda r: r['score'])


@app.route('/process_query', methods=['POST', 'GET'])
def process_query():
    print('processing query....')
    q = request.args.get("text")
    f = request.args
    
    print(q, f)

    #return json.dumps({})

    res = get_users(q, f)
    print('done!')

    return json.dumps(res)


def start_app(config):
    port = config['SERVER_INFO'].getint('PORT')
    ip = config['SERVER_INFO'].get('IP')
    
    print('starting a server, IP: {}, PORT: {}'.format(ip, port))
    
    app.run(host=ip, port=port)
    #socketio = SocketIO(app)
    #socketio.run(app)

if __name__ == '__main__':
    config = configparser.ConfigParser()
    config.read('config.ini')

    start_app(config)
