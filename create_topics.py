import os
import pickle 
import re
import gc

import numpy as np

from pymorphy2 import MorphAnalyzer

from sklearn.feature_extraction.text import TfidfVectorizer, CountVectorizer, HashingVectorizer
from sklearn.decomposition import LatentDirichletAllocation

from nltk.corpus import stopwords
from nltk.tokenize import RegexpTokenizer


from pymongo import MongoClient

from collections import defaultdict

import tqdm

from sklearn.manifold import TSNE
from sklearn.decomposition import PCA

import matplotlib.pyplot as plt
import seaborn as sns

import time

from utility import Stemmizer

def read_stops():
    res = set()
    with open('stops.txt') as in_file:
        for line in in_file:
            res.add(line.strip())
    
    return res

s = Stemmizer()

def create_or_load_ids_dict(db):
    if os.path.exists('ids_indices_dict.pickle'):
        with open('ids_indices_dict.pickle', 'rb') as handle:
            return pickle.load(handle)

    ids = [user['uid'] for user in db.users.find()]
    indices = list(range(len(ids)))

    dictionary = dict(zip(ids, indices))
    dictionary.update(zip(indices, ids))

    with open('ids_indices_dict.pickle', 'wb') as handle:
        pickle.dump(dictionary, handle)

    return dictionary

def create_or_load_users_to_posts(db, ids_indices_dict):
    if os.path.exists('users_to_posts.pickle'):
        with open('users_to_posts.pickle', 'rb') as handle:
            return pickle.load(handle)

    users_to_posts = defaultdict(str)

    for user_posts in tqdm.tqdm_notebook(db.wall_posts.find(), total=db.wall_posts.count()):
        for p in user_posts['posts']:
            users_to_posts[ids_indices_dict[user_posts['uid']]] += p['text']

    with open('users_to_posts.pickle', 'wb') as handle:
        pickle.dump(users_to_posts, handle)

    return users_to_posts


def stemming(text):    
    return s.process(text)

def links_content_map():
    res = defaultdict(list)
    
    for link in db.links_content.find():
        res[link['url']].append(link)
    
    return res


def user_info_map():
    res = dict()
    
    for u in db.user_info.find():
        res[u['uid']] = u
    
    return res


def user_links_map():
    res = defaultdict(list)
    
    for user_links in db.links.find():
        res[user_links['uid']].append(user_links)
    
    return res


def process_user(db, uid, users_to_posts, ids_indices_dict, links_content, user_links_m, user_info):
    text = ''
    
    def not_none(value):
        return value if value is not None else ''

    for user_links in user_links_m[uid]:
        for user_link in user_links['links']:
            for link in links_content[user_link]:
                if link['type'] == 'sprashivai':
                    text += ' '.join(not_none(link['answers']))
                elif link['type'] == 'livejournal' or link['type'] == 'pikabu':
                    text += ' ' + not_none(link['title'])
                    text += ' ' + not_none(link['text'])
                elif link['type'] == 'youtube':
                    text += ' ' + not_none(link['description'])
                    text += ' '.join(not_none(link['tags']))
                    text += ' ' + not_none(link['name'])
                elif link['type'] == 'ali':
                    text += ' ' + not_none(link['name'])
                elif link['type'] == 'ask':
                    text += ' '.join(not_none(link['answers']))
                elif link['type'] == 'unknown':
                    text += ' ' + not_none(link['description'])
                    text += ' ' + not_none(link['title'])
    
    if uid in user_info:
        u = defaultdict(str, user_info[uid])

        text += ' ' + u['about']
        text += ' ' + u['quotes']
        text += ' ' + u['activities']
        text += ' ' + u['interests']
        text += ' ' + u['music']
        text += ' ' + u['movies']
        text += ' ' + u['tv']
        text += ' ' + u['books']

        text += ' ' + users_to_posts[ids_indices_dict[uid]]
    
    return stemming(text)


client = MongoClient()
db = client.ir_project

def read_or_create_texts():
    try:
        with open('users_texts.bin', 'rb') as in_file:
            return pickle.load(in_file)
    except:
        ids = create_or_load_ids_dict(db)
        users_to_posts = create_or_load_users_to_posts(db, ids)
        user_info = user_info_map()
        
        links_content = links_content_map()
        user_links = user_links_map()
        
        print('Total links parsed: {}'.format(len(links_content)))
        
        users_texts = dict()
        total=db.users.count()
        
        timer_step = 10
        times = list()
        
        for i, u in enumerate(db.users.find(no_cursor_timeout=True)):
            start = time.time()
            users_texts[u['uid']] = process_user(db, u['uid'], users_to_posts, ids, links_content, user_links, user_info)
            end = time.time()
            func_time = end - start
            times.append(func_time)
            if i % 10 == 0:
                eta = (total - i + 1)/timer_step*np.mean(times)
                print('Processed: {}/{}, ETA: {:.3f}m, time per {} iterations: {:.3f}'
                      .format(i, total, eta/60, timer_step, np.mean(times)), end='\r')
                
        with open('users_texts.bin', 'wb') as out:
            pickle.dump(users_texts, out)
            
        return users_texts
    
users_texts = read_or_create_texts()

users_texts = {uid:text for uid, text in users_texts.items() if len(text) > 0}

only_texts = [t for t in users_texts.values()]

TOP_WORDS = 10
TOPICS_COUNT = 25

def print_top_words(model, feature_names, n_top_words):
    for topic_idx, topic in enumerate(model.components_):
        print("Topic #%d:" % topic_idx)
        print(" ".join([feature_names[i]
                        for i in topic.argsort()[:-n_top_words - 1:-1]]))
    print()
    
tf_vectorizer = CountVectorizer(min_df = 7, max_df = 0.9, stop_words=s.stops)
tf = tf_vectorizer.fit_transform(only_texts)

pickle.dump(tf_vectorizer, open('vect.bin', 'wb')) 