import time
import argparse
import pprint
import pickle
import tqdm
import re
import os
import gensim
import multiprocessing

from pymongo import MongoClient
from pymorphy2 import MorphAnalyzer
from nltk.corpus import stopwords
from collections import defaultdict
from multiprocessing.dummy import Pool
from threading import Lock
from gensim.models import Doc2Vec
from gensim.models.doc2vec import TaggedDocument
from collections import Counter


from utility import Stemmizer

s = Stemmizer()

class Index:
    def __init__(self, db):
        self.db = db
        self.mutex = Lock()
        self.pool = Pool(processes=1)
        self.ids_indices_dict = Index.create_or_load_ids_dict(self.db)
        self.reverse_index = defaultdict(Counter)
        self.dl = dict()

        self.forward_index = db.forward_index
        self.morpher = MorphAnalyzer()

        total_users = self.db.users.count()

        self.pbar = tqdm.tqdm(total=total_users)
        self.users_to_posts = None

    def build_forward_and_reverse(self):
        uids = [user['uid'] for user in self.db.users.find()]

        self.users_to_posts = self.create_or_load_users_to_posts()

        self.pool.map(self.process_user, uids)

        with open('reverse_index.pickle', 'wb') as handle:
            pickle.dump(self.reverse_index, handle)

        with open('doc_length.pickle', 'wb') as handle:
            pickle.dump(self.dl, handle)

    def create_or_load_users_to_posts(self):
        if os.path.exists('users_to_posts.pickle'):
            with open('users_to_posts.pickle', 'rb') as handle:
                return pickle.load(handle)

        users_to_posts = defaultdict(str)

        for user_post in tqdm.tqdm(self.db.wall_posts.find(), total=self.db.wall_posts.count()):
            users_to_posts[self.ids_indices_dict[user_post['from_id']]] += user_post['text']

        with open('users_to_posts.pickle', 'wb') as handle:
            pickle.dump(users_to_posts, handle)

        return users_to_posts

    def build_reverse(self):
        for forward in tqdm.tqdm(self.forward_index.find(), total=self.forward_index.count()):
            splitted = forward['text'].split()
            uid = forward['uid']

            for token in splitted:
                self.update_reverse(token, self.ids_indices_dict[uid])

            self.pbar.update(1)

        with open('reverse_index.pickle', 'wb') as handle:
            pickle.dump(self.reverse_index, handle)

    def update_reverse(self, token, index):
        self.reverse_index[token][index] += 1

    def get_reverse_index(self):
        if not os.path.exists('reverse_index.pickle'):
            raise FileExistsError('reverse index is not found')
        with open('reverse_index.pickle', 'rb') as handle:
            return pickle.load(handle)

    def get_ids_dict(self):
        return Index.create_or_load_ids_dict(self.db)

    @staticmethod
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

    def process_user(self, uid):
        text = ''

        def not_none(value):
            return value if value is not None else ''

        for user_links in self.db.links.find({'uid': uid}):
            for user_link in user_links['links']:
                for link in self.db.links_content.find({'url': user_link}):
                    if link['type'] == 'sprashivai':
                        text += ' '.join(not_none(link['answers']))
                    elif link['type'] == 'livejournal' or link['type'] == 'pikabu':
                        text += not_none(link['title'])
                        text += not_none(link['text'])
                    elif link['type'] == 'youtube':
                        text += not_none(link['description'])
                        text += ' '.join(not_none(link['tags']))
                        text += not_none(link['name'])
                    elif link['type'] == 'ali':
                        text += not_none(link['name'])
                    elif link['type'] == 'ask':
                        text += ' '.join(not_none(link['answers']))
                    elif link['type'] == 'unknown':
                        text += not_none(link['description'])
                        text += not_none(link['title'])

        text += self.users_to_posts[self.ids_indices_dict[uid]]

        text = s.process(text)

        self.forward_index.insert_one({'uid': uid, 'text': text})

        splitted = text.split()

        self.mutex.acquire()
        self.dl[self.ids_indices_dict[uid]] = len(splitted)
        for token in splitted:
            self.update_reverse(token, self.ids_indices_dict[uid])
        self.pbar.update(1)
        self.mutex.release()


def build_doc2vec(db):
    cores = multiprocessing.cpu_count()
    assert gensim.models.doc2vec.FAST_VERSION > -1, "This will be painfully slow otherwise"
    #
    # forward_index = db.forward_index
    #
    # ids_indices_dict = Index.create_or_load_ids_dict(db)
    #
    # documents = []
    #
    # for id_text in forward_index.find():
    #     documents.append(TaggedDocument(id_text['text'].split(), [ids_indices_dict[id_text['uid']]]))
    with open('doc.txt', 'r') as handle:
        lines = handle.readlines()
        print(len(lines))
        lines = ' '.join(lines)
        lines = lines.split()

        i = 0
        docs = []
        while i < len(lines):
            docs.append(lines[i: i + 20])
            i += 20

        documents = [TaggedDocument(doc, 'sent{}'.format(index)) for (index, doc) in enumerate(docs)]

    model = Doc2Vec(documents, size=100, window=10, min_count=5, workers=cores)

    # for epoch in range(10):
    #     model.train(documents[epoch], total_words=1000, epochs=10)

    model.save('forward_index.doc2vec')


def main(args):
    client = MongoClient()
    db = client.ir_project

    index = Index(db=db)

    index.build_forward_and_reverse()

    # db = None
    #
    # if args.doc2vec:
    #     build_doc2vec(db)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Script is supposed to build forward and reverse index')
    parser.add_argument('--doc2vec', action='store_true', default=False,
                        help='Build doc2vec as well?')

    args = parser.parse_args()

    main(args)
