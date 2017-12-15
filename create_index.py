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
        self.users_to_links = None
        self.links_to_contents = None
        self.users_to_texts = dict()

    def link_to_content(self):
        res = defaultdict(list)
        for link in self.db.links_content.find():
            res[link['url']].append(link)
        return res

    def user_to_links(self):
        res = defaultdict(list)
        for user_links in self.db.links.find():
            res[self.ids_indices_dict[user_links['uid']]].append(user_links)
        return res

    def build_forward_and_reverse(self):
        uids = [user['uid'] for user in self.db.users.find()]

        self.users_to_posts = self.create_or_load_users_to_posts()
        self.users_to_links = self.create_or_load_users_to_links()
        self.links_to_contents = self.create_or_load_links_to_contents()

        self.pool.map(self.process_user, uids)

#        with open('reverse_index.pickle', 'wb') as handle:
#            pickle.dump(self.reverse_index, handle)

#        with open('doc_length.pickle', 'wb') as handle:
#            pickle.dump(self.dl, handle)

        with open('users_to_texts.pickle', 'wb') as handle:
            pickle.dump(self.users_to_texts, handle)


    def create_or_load_users_to_links(self):
        if os.path.exists('users_to_links.pickle'):
            with open('users_to_links.pickle', 'rb') as handle:
                return pickle.load(handle)

        users_to_links = defaultdict(list)

        for user_links in self.db.links.find():
            try:
                users_to_links[self.ids_indices_dict[user_links['uid']]].extend(user_links['links'])
            except KeyError:
                continue
            self.pbar.update(1)

        with open('users_to_links.pickle', 'wb') as handle:
            pickle.dump(users_to_links, handle)

        return users_to_links

    def create_or_load_links_to_contents(self):
        if os.path.exists('links_to_contents.pickle'):
            with open('links_to_contents.pickle', 'rb') as handle:
                return pickle.load(handle)

        links_to_contents = defaultdict(str)

        def not_none(value):
            return value if value is not None else ''

        for link in self.db.links_content.find():
            text = '' 
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
            links_to_contents[link['url']] = text

        with open('links_to_contents.pickle', 'wb') as handle:
            pickle.dump(links_to_contents, handle)

        return links_to_contents


    def create_or_load_users_to_posts(self):
        if os.path.exists('users_to_posts.pickle'):
            with open('users_to_posts.pickle', 'rb') as handle:
                return pickle.load(handle)

        users_to_posts = defaultdict(str)

        for user_post in tqdm.tqdm(self.db.wall_posts.find(), total=self.db.wall_posts.count()):
            user_text = ''
            for p in user_post['posts'][:50]:
                user_text += p['text']
            users_to_posts[self.ids_indices_dict[user_post['uid']]] = user_text

        with open('users_to_posts.pickle', 'wb') as handle:
            pickle.dump(users_to_posts, handle)

        return users_to_posts

    def build_reverse(self):
        uids = [user['uid'] for user in self.db.users.find()]
#        users_to_texts_stem = dict()
        with open('users_to_texts.pickle', 'rb') as handle:
            self.users_to_texts = pickle.load(handle)

#        print(len(uids))

        for uid in uids:
            try:
                text = self.users_to_texts[uid]
            except KeyError:
                continue
            text = s.process(text)
            splitted = text.split()
            self.dl[self.ids_indices_dict[uid]] = len(splitted)
            self.users_to_texts[uid] = text
#            for token in splitted:
#                self.update_reverse(token, self.ids_indices_dict[uid])

            self.pbar.update(1)

        with open('doc_length.pickle', 'wb') as handle:
            pickle.dump(self.dl, handle)

        with open('users_to_texts_stem.pickle', 'wb') as handle:
            pickle.dump(self.users_to_texts, handle)


#        with open('reverse_index.pickle', 'wb') as handle:
#            pickle.dump(self.reverse_index, handle)

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

        try:
            for link in self.users_to_links[self.ids_indices_dict[uid]]:
                text += self.links_to_contents[link]
        except KeyError:
            pass

        text += self.users_to_posts[self.ids_indices_dict[uid]]

#        self.users_to_texts[self.ids_indices_dict[uid]] = text
        self.users_to_texts[uid] = text

#        text = s.process(text)

#	self.users_to_texts[self.ids_indices_dict[uid]] = text
#        self.forward_index.insert_one({'uid': uid, 'text': text})

#        splitted = text.split()

#        self.mutex.acquire()
#        self.dl[self.ids_indices_dict[uid]] = len(splitted)
#        for token in splitted:
#            self.update_reverse(token, self.ids_indices_dict[uid])
        self.pbar.update(1)
#        self.mutex.release()


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
    print(db)

    index = Index(db=db)

    index.build_reverse()
#    index.build_forward_and_reverse()
#    index.create_or_load_users_to_links()
#    index.create_or_load_links_to_contents()
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
