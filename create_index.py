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
from gensim.models.doc2vec import TaggedDocument
from collections import Counter
from os import listdir
from os.path import isfile, join
from utils import IndexFiles, Stemmer


class ForwardIndex:
    CHUNK_SIZE = 25000
    MAX_PROC = os.cpu_count()

    def __init__(self):
        subprocess.run('sudo service mongod start'.split())
        time.sleep(5)
        client = MongoClient()
        self.data_base = client.ir_project

    def build(self):
        self.build_raws(IndexFiles.TMP_DIR, IndexFiles.RAW_PAT)
        subprocess.run('sudo service mongod stop'.split())
        ForwardIndex.clean_raws(IndexFiles.TMP_DIR)
        ForwardIndex.merge_cleans(IndexFiles.TMP_DIR, IndexFiles.FORWARD_INDEX)

    def build_raws(self, directory, file_pattern):
        uids = [user['uid'] for user in self.data_base.users.find()]
        offset = ForwardIndex.CHUNK_SIZE
        i = 0

        users_to_links = self.get_users_to_links()
        links_to_contents = self.get_links_to_contents()

        while i < math.ceil(len(uids) / offset):
            local_uids = uids[i * offset: (i + 1) * offset]
            forward_index = self.get_users_to_posts(i, local_uids)

            print('Merging user info chunk {:03} together:'.format(i))

            for uid in tqdm.tqdm(local_uids, total=len(local_uids)):
                text = ''
                for link in users_to_links[uid]:
                    text += ' ' + links_to_contents[link]
                forward_index[uid] += ' ' + text

            IndexFiles.dump(directory + file_pattern.format(i), forward_index)
            i += 1

    @staticmethod
    def clean_raws(directory):
        raw_files = [join(directory, f) for f in listdir(directory) if
                     isfile(join(directory, f)) and 'raw' in f]
        sizes = [os.path.getsize(file) for file in raw_files]

        files_with_sizes = list(zip(sizes, raw_files))
        files_with_sizes.sort(reverse=True)

        files = [file for _, file in files_with_sizes]

        i = 0
        offset = ForwardIndex.MAX_PROC

        while i * offset < len(files):
            with Pool() as pool:
                pool.map(ForwardIndex.process_chunk, files[i * offset: (i + 1) * offset])
            i += 1

    @staticmethod
    def merge_cleans(dir, filename):
        clean_files = [join(dir, f) for f in listdir(dir) if isfile(join(dir, f)) and 'clean' in f]

        forward_index = dict()

        for file in clean_files:
            tmp_index = IndexFiles.load(file)
            forward_index.update(tmp_index)

        IndexFiles.dump(filename, forward_index)

    @staticmethod
    def process_chunk(filepath):
        stemmer = Stemmer()
        chunk = IndexFiles.load(filepath)

        for uid in chunk:
            chunk[uid] = stemmer.process(chunk[uid])

        IndexFiles.dump(filepath.replace('raw', 'clean'), chunk)

    def get_users_to_posts(self, chunk_id, uids):
        users_to_posts = defaultdict(str)

        print('Loading user chunk {:03} posts:'.format(chunk_id))

        for user_post in tqdm.tqdm(self.data_base.wall_posts.find({'uid': {'$in': uids}}),
                                   total=len(uids)):
            user_text = ''
            for p in user_post['posts']:
                user_text += ' ' + p['text']
            users_to_posts[user_post['uid']] = user_text

        return users_to_posts

    def get_links_to_contents(self):
        links_to_contents = defaultdict(str)

        def not_none(value):
            return value if value is not None else ''

        print('Loading link contents:')

        for link in tqdm.tqdm(self.data_base.links_content.find(),
                              total=self.data_base.links_content.count()):
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

    def get_users_to_links(self):
        users_to_links = defaultdict(list)

        print('Loading user links:')

        for user_links in tqdm.tqdm(self.data_base.links.find(),
                                    total=self.data_base.links.count()):
            users_to_links[user_links['uid']].extend(user_links['links'])

        return users_to_links


class ReverseIndex:
    def __init__(self):
        subprocess.run('sudo service mongod start'.split())
        time.sleep(5)
        client = MongoClient()
        self.data_base = client.ir_project
        total_users = self.data_base.users.count()
        self.pbar = tqdm.tqdm(total=total_users)
        self.reverse_index = defaultdict(Counter)
        self.dl = dict()
        subprocess.run('sudo service mongod stop'.split())

    def build(self):
        clean_files = [join(IndexFiles.TMP_DIR, f)
                       for f in listdir(IndexFiles.TMP_DIR)
                       if isfile(join(IndexFiles.TMP_DIR, f)) and 'clean' in f]

        for file in clean_files:
            forward_index = IndexFiles.load(file)
            for uid in forward_index:
                splitted = forward_index[uid].split()
                self.dl[uid] = len(splitted)
                [self.update_reverse(token, uid) for token in splitted]
                self.pbar.update(1)

        IndexFiles.dump(IndexFiles.DOC_LENGTH, self.dl)
        IndexFiles.dump(IndexFiles.REVERSE_INDEX, self.reverse_index)

    def update_reverse(self, token, index):
        self.reverse_index[token][index] += 1


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

    if args.action == 'build_forward':
        print('Building forward index:')
        forward_index = ForwardIndex()
        forward_index.build()

    elif args.action == 'build_reverse':
        print('Building reverse index:')
        reverse_index = ReverseIndex()
        reverse_index.build()

    elif args.action == 'build_all':
        print('Building both indices:')
        print('Building forward index:')
        start_time = time.time()
        forward_index = ForwardIndex()
        forward_index.build()
        middle_time = time.time()
        print('elapsed time for building forward index: {} min'.format((middle_time - start_time) / 60))
        print('Building reverse index:')
        reverse_index = ReverseIndex()
        reverse_index.build()
        end_time = time.time()
        print('elapsed time for building reverse index: {} min'.format((end_time - middle_time) / 60))

    elif args.action == 'clean':
        IndexFiles.clean_tmp(IndexFiles.TMP_DIR)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Script is supposed to build forward and reverse index')
    parser.add_argument('--action', type=str, default='build_all',
                        help='')

    arguments = parser.parse_args()
    main(arguments)
