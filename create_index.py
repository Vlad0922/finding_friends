import time
import argparse
import tqdm
import os
import gensim
import multiprocessing
import subprocess
import math

from pymongo import MongoClient
from collections import defaultdict
from multiprocessing import Pool
from gensim.models import Doc2Vec
from gensim.models.doc2vec import LabeledSentence, TaggedDocument
from collections import Counter
from os import listdir
from os.path import isfile, join
from utils import IndexFiles, Stemmer, Timer, MongoManager, get_chunks
from itertools import accumulate, repeat, chain, tee
from functools import partial


class ForwardIndex:
    CHUNK_SIZE = 12000
    MAX_PROC = os.cpu_count()

    def __init__(self):
        subprocess.run('sudo service mongod start'.split())
        time.sleep(5)
        client = MongoClient()
        self.data_base = client.ir_project

    @staticmethod
    def get_chunks(cont, width):
        slices = accumulate(chain((0,), repeat(width, math.ceil(len(cont) / width))))
        begin, end = tee(slices)
        next(end)
        return [cont[slc] for slc in map(slice, begin, end)]

    def build(self):
        self.data_base.forward_index.delete_many({})

        uids = [int(user['uid']) for user in self.data_base.users.find()]
        uids = ForwardIndex.get_chunks(uids, ForwardIndex.CHUNK_SIZE)
        uids = ForwardIndex.get_chunks(uids, ForwardIndex.MAX_PROC)

        for uids_chunk in uids:
            with Pool() as pool:
                pool.map(ForwardIndex.process_chunk_, uids_chunk)

    @staticmethod
    def process_chunk_(uids):
        stemmer = Stemmer()

        client = MongoClient()
        db_client = client.ir_project

        forward_index = ForwardIndex.get_users_to_posts_(db_client, uids)
        links_to_contents = ForwardIndex.get_links_to_contents_(db_client, uids)
        users_to_links = ForwardIndex.get_users_to_links_(db_client, uids)
        users_to_infos = ForwardIndex.get_users_to_infos_(db_client, uids)

        for uid in uids:
            text = ''
            for link in users_to_links[uid]:
                text += ' ' + links_to_contents[link]
            forward_index[uid] = text + ' ' + users_to_infos[uid] + ' ' + forward_index[uid]
            forward_index[uid] = stemmer.process(forward_index[uid])
            if not 10 < len(forward_index[uid]) < 10000:
                del forward_index[uid]

        db_insertions = [{'uid': uid, 'text': forward_index[uid]} for uid in forward_index]
        db_client.forward_index.insert_many(db_insertions)

    @staticmethod
    def get_users_to_posts_(db_client, uids):
        users_to_posts = defaultdict(str)

        with Timer('Loading users to posts'):
            for user_post in db_client.wall_posts.find({'uid': {'$in': uids}}):
                user_text = ''
                for p in user_post['posts']:
                    user_text += ' ' + p['text']
                users_to_posts[int(user_post['uid'])] = user_text

        return users_to_posts

    @staticmethod
    def get_links_to_contents_(db_client, uids):
        links_to_contents = defaultdict(str)

        def not_none(value):
            return value if value is not None else ''

        with Timer('Loading link content'):
            for link in db_client.links_content.find({'uid': {'$in': uids}}):
                text = ''
                if link['type'] == 'sprashivai':
                    text += ' ' + ' '.join(not_none(link['answers']))
                elif link['type'] == 'livejournal' or link['type'] == 'pikabu':
                    text += ' ' + not_none(link['title'])
                    text += ' ' + not_none(link['text'])
                elif link['type'] == 'youtube':
                    text += ' ' + not_none(link['description'])
                    text += ' ' + ' '.join(not_none(link['tags']))
                    text += ' ' + not_none(link['name'])
                elif link['type'] == 'ali':
                    text += ' ' + not_none(link['name'])
                elif link['type'] == 'ask':
                    text += ' ' + ' '.join(not_none(link['answers']))
                elif link['type'] == 'unknown':
                    text += ' ' + not_none(link['description'])
                    text += ' ' + not_none(link['title'])
                links_to_contents[link['url']] = text

        return links_to_contents

    @staticmethod
    def get_users_to_links_(db_client, uids):
        users_to_links = defaultdict(list)
        with Timer('Loading user to links'):
            for user_links in db_client.links.find({'uid': {'$in': uids}}):
                users_to_links[int(user_links['uid'])].extend(user_links['links'])
        return users_to_links

    @staticmethod
    def get_users_to_infos_(db_client, uids):
        users_to_infos = defaultdict(str)

        infos = ['interests', 'music', 'activities', 'movies',
                 'tv', 'books', 'games', 'about', 'quotes']

        with Timer('Loading user infos'):
            for user in db_client.user_info.find({'uid': {'$in': uids}}):
                user = defaultdict(str, user)
                user_text = ''
                for info in infos:
                    user_text += ' ' + user[info]
                users_to_infos[int(user['uid'])] = user_text
        return users_to_infos

    @staticmethod
    def compress():
        with Timer('Compressing forward index'), MongoManager():
            client = MongoClient()
            data_base = client.ir_project
            forward_index_comp = dict()

            result = data_base.forward_index_comp.delete_many({})
            print(result.deleted_count)

            for entry in tqdm.tqdm(data_base.forward_index.find(),
                                   total=data_base.forward_index.count()):
                if 500 < len(entry['text']) < 1000:
                    forward_index_comp[entry['uid']] = entry['text']

            db_insertions = [{'uid': uid, 'text': forward_index_comp[uid]} for uid in forward_index_comp]
            data_base.forward_index_comp.insert_many(db_insertions)

            print('forward_index_comp size: {}'.format(data_base.forward_index_comp.count()))


class ReverseIndex:
    CHUNK_SIZE = 1024

    def __init__(self):
        subprocess.run('sudo service mongod start'.split())
        time.sleep(5)
        client = MongoClient()
        self.data_base = client.ir_project
        self.reverse_index = defaultdict(Counter)
        self.user_length = dict()

    def build(self):
        result = self.data_base.reverse_index.delete_many({})
        print(result.deleted_count)
        result = self.data_base.user_length.delete_many({})
        print(result.deleted_count)

        for user in tqdm.tqdm(self.data_base.forward_index.find(), total=self.data_base.forward_index.count()):
            splitted = user['text'].split()
            self.user_length[user['uid']] = len(splitted)
            [self.update_reverse(token, user['uid']) for token in splitted]

        subprocess.run('sudo service mongod stop'.split())
        time.sleep(2)
        subprocess.run('sudo service mongod start'.split())
        time.sleep(2)

        token_chunks = get_chunks(list(self.reverse_index.keys()), ReverseIndex.CHUNK_SIZE)

        for chunk in tqdm.tqdm(token_chunks, total=len(token_chunks)):
            local_index = dict()
            for token in chunk:
                local_index[token] = list(zip(self.reverse_index[token].keys(),
                                              self.reverse_index[token].values()))

            chunk_insertion = [{'token': token, 'uids_freqs': local_index[token]} for token in local_index]
            self.data_base.reverse_index.insert_many(chunk_insertion)

        length_insertions = [{'uid': uid, 'length':  self.user_length[uid]} for uid in self.user_length]
        self.data_base.user_length.insert_many(length_insertions)

    def update_reverse(self, token, uid):
        self.reverse_index[token][uid] += 1


class SearchData:
    def __init__(self):
        client = MongoClient()
        self.data_base = client.ir_project

    def build(self):
        # with Timer('Building forward index with mongo'):
        #     forward_index = ForwardIndex()
        #     forward_index.build()

        # forward_index = ForwardIndex()
        # forward_index.compress()

        # with Timer('Building reverse index with mongo'):
        #     reverse_index = ReverseIndex()
        #     reverse_index.build()
        # self.build_token_freqs()
        self.build_users_infos()

    @staticmethod
    def build_reverse_index():
        with Timer('Building reverse index'):
            reverse_index = ReverseIndex()
            reverse_index.build()

    @staticmethod
    def build_forward_index():
        with Timer('Building forward index'):
            forward_index = ForwardIndex()
            forward_index.build()

    def build_token_freqs(self):
        with Timer('Building token frequencies'), MongoManager():
            client = MongoClient()
            data_base = client.ir_project
            result = self.data_base.token_freqs.delete_many({})
            print(result.deleted_count)

            token_freqs = Counter()
            for entry in tqdm.tqdm(self.data_base.reverse_index.find(), total=self.data_base.reverse_index.count()):
                for _, freq in entry['uids_freqs']:
                    token_freqs[entry['token']] += freq

            token_freqs_insertions = [{'token': token, 'freq': token_freqs[token]} for token in token_freqs]
            data_base.token_freqs.insert_many(token_freqs_insertions)

    def build_users_infos(self):
        with Timer('Building user infos'), MongoManager():
            client = MongoClient()
            data_base = client.ir_project
            result = self.data_base.users_infos.delete_many({})
            print(result.deleted_count)

            users_infos = defaultdict(dict)

            count1 = 0
            count2 = 0

            for user in tqdm.tqdm(self.data_base.users.find(), total=self.data_base.users.count()):
                uid = int(user['uid'])
                users_infos[uid]['sex'] = user['sex']
                users_infos[uid]['city'] = user['city']
                users_infos[uid]['age'] = user['age']
                try:
                    users_infos[uid]['relation'] = user['relation']
                    count1 += 1
                except KeyError:
                    users_infos[uid]['relation'] = -1
                    count2 += 1

            print(count1, count2)

            users_infos_insertions = [{'uid': uid,
                                       'sex': users_infos[uid]['sex'],
                                       'city': users_infos[uid]['city'],
                                       'age': users_infos[uid]['age'],
                                       'relation': users_infos[uid]['relation']} for uid in users_infos]
            data_base.users_infos.insert_many(users_infos_insertions)


def build_doc2vec():
    cores = multiprocessing.cpu_count()
    assert gensim.models.doc2vec.FAST_VERSION > -1, "This will be painfully slow otherwise"
    #
    # def generate_docs(db):
    #     # count = 0
    #     for entry in db.forward_index_comp.find():
    #         yield TaggedDocument(entry['text'].split(), [entry['uid']])
    #         # if count >= 1000:
    #         #     break
    #         # count += 1

    client = MongoClient()
    database = client.ir_project

    model = Doc2Vec(size=100, workers=cores, alpha=0.025, min_alpha=0.025)

    docs = [TaggedDocument(entry['text'].split()[:48], [entry['uid']]) for entry in database.forward_index.find()]

    docs = docs[:256]
    print(len(docs))

    print('docs are loaded')

    subprocess.run('sudo service mongod stop'.split())

    # it = generate_docs(database)

    model.build_vocab(docs)

    for epoch in range(10):
        print('epoch: {}'.format(epoch))
        model.train(docs, total_examples=model.corpus_count)
        model.alpha -= 0.002
        model.min_alpha = model.alpha
        model.train(docs)

    model.save('index.doc2vec')


def main(args):

    if args.action == 'build_forward':
        print('Building forward index:')
        forward_index = ForwardIndex()
        forward_index.build()

    elif args.action == 'build_reverse':
        print('Building reverse index:')
        reverse_index = ReverseIndex()
        reverse_index.build()

    elif args.action == 'build_all':
        print('Building data for search:')
        search_data = SearchData()
        search_data.build()

    elif args.action == 'build_doc2vec':
        print('Building doc2vec:')
        with MongoManager():
            build_doc2vec()

    elif args.action == 'clean':
        IndexFiles.clean_tmp(IndexFiles.TMP_DIR)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Script is supposed to build forward and reverse index')
    parser.add_argument('--action', type=str, default='build_doc2vec',
                        help='')

    arguments = parser.parse_args()
    main(arguments)
