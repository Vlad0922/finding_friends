import sys
sys.path.insert(0,'..')

import numpy as np

from collections import defaultdict

from flask_socketio import SocketIO
from flask import Flask, render_template, make_response, request, Response

from gensim.models.ldamulticore import LdaMulticore

from pymongo import MongoClient

import time

import json

import configparser

from search import SearchEngine

import pickle

from download_users import load_user_text


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


def preload_topics_words():
    print('loading a topics terms...')
    start_t = time.time()
   
    res = list()
    for i in range(100):
        terms = [ldamodel.id2word[t[0]] for t in ldamodel.get_topic_terms(i)]
        res.append(terms)
    
    print('done: {}s'.format(round(time.time() - start_t, 3)))

    return res

def preload_lda_model():
    print('loading the lda model...')
    start_t = time.time()

    lda = pickle.load(open('gensim_lda.pickle', 'rb'))
    dictionary =  pickle.load(open('../gensim_dict.pickle', 'rb'))

    print('done: {}s'.format(round(time.time() - start_t, 3)))

    return dictionary, lda


def preload_main_info():
    print('loading main info...')
    start_t = time.time()

    res = defaultdict(dict)

    for u in db.users.find():
        res[u['uid']] = u

    for u in db.user_info.find():
        res[u['uid']].update(u)

    for k in res:
        del res[k]['_id']
    
    print('done: {}s'.format(round(time.time() - start_t, 3)))

    return res


def preload_topics():
    print('loading topics...')
    start_t = time.time()

    res = dict()

    for t in db.topics.find():
        res[t['uid']] = t['topics']
    
    print('done: {}s'.format(round(time.time() - start_t, 3))) 
    return res


def preload_forward_index():
    print('loading forward index...')
    start_t = time.time()

    res = json.load(open('../forward_index.json'))

    print('done: {}s'.format(round(time.time() - start_t, 3)))

    return res


main_info = preload_main_info()
ldadict, ldamodel = preload_lda_model()
topics_words = preload_topics_words()
topics = preload_topics()
#forward_index = preload_forward_index()


def get_user_text(uid):
    #global forward_index

    #if str(uid) in forward_index:
    #    return forward_index[uid]
    
    user_text = load_user_text(uid)
    #forward_index[str(uid)] = user_text
    
    return eng.stemmer.process(user_text)


def get_query_topic_words(query):
    topics = ldamodel[ldadict.doc2bow(query.split())]
    
    words = str()
    for t in topics:
        words += ' '.join(topics_words[t[0]])

    return words
    #return ' '.join([topics_words[t[0]] for t in topics])


@app.route('/')
def hello_world():
    print('wow! a request for an index!')
    return render_template('index.html')


def topics_to_heatmap(topics):
    res = np.zeros((10, 10))
       
    for t_idx, t_val in topics:
        res[t_idx % 10, t_idx // 10] = t_val

    return res.tolist()


def get_users(text, filters, count=10):
    start_t = time.time()
    search_res = {int(uid):score for uid,score in eng.search('BM25', 'search', text, 10, int(filters['gender']), int(filters['city']), int(filters['age_from']), int(filters['age_to']), int(filters['status']))}
    print(search_res)
    print('Search time: {}s'.format(round(time.time() - start_t, 3)))

    start_t = time.time()
    max_score = max([val for val in search_res.values()], default=1e-5)
    
    for uid in search_res:
        search_res[uid] = round(search_res[uid] / max_score, 4)

    #res = [main_info[uid] for uid in db.users.find({'uid': {'$in': list(search_res.keys())}})]
    res = [main_info[uid] for uid in search_res.keys()]
    #photos = defaultdict(lambda: 'https://vk.com/images/camera_200.png',
    #                        {u['uid']:u['photo_max_orig'] for u in db.user_info.find({"uid": {"$in": [r['uid'] for r in res]}})}) 
    #topics = defaultdict(lambda: np.zeros(25).tolist(), get_topics([r['uid'] for r in res]))

    for r in res:
        #r['photo'] = photos[r['uid']]
        r['sex'] = gender_map[r['sex']]
        r['city'] = city_map[r['city']]
        r['topics_heatmap'] = topics_to_heatmap(topics[r['uid']])
        r['topics'] = sorted(topics[r['uid']], key=lambda t: t[1])
        r['topics_words'] = [' '.join(topics_words[t[0]]) for t in  r['topics']]
        r['score'] = search_res[r['uid']]

        #del r['_id']
    print('preprocessing time: {}s'.format(round(time.time() - start_t, 3)))

    return sorted(res, key=lambda r: r['score'])


def get_recommendations(text, filters, count=10):
    print('loading recommendations...')

    start_t = time.time()
    
    uid = int(text[2:])
    user_text = get_user_text(uid)
    query = get_query_topic_words(user_text)

    res = get_users(query, filters, count)
    
    print('done: {}s'.format(round(time.time() - start_t, 3)))

    return res


@app.route('/process_query', methods=['POST', 'GET'])
def process_query():
    print('processing query....')
    q = request.args.get("text")
    f = request.args
    print(f)
    if q.startswith('id'):
        res = get_recommendations(q, f)
    else:
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
