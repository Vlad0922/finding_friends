import download_users
import time
import argparse
import pickle
import os
import math
import tqdm

from gensim.models import Doc2Vec
from collections import Counter

from utils import IndexFiles, Stemmer


class BM25:
    def __init__(self):
        self.reverse_index = None
        self.dl = None
        self.df = None
        self.bm25 = None

        self.k1 = 1.5
        self.b = 0.75

        self.stemmer = Stemmer()

        self.load_data()

        self.N = len(self.dl)
        self.d_avg = sum(list(self.dl.values())) / self.N

    def load_data(self):
        self.reverse_index = IndexFiles.load(IndexFiles.REVERSE_INDEX)
        for index, token in enumerate(self.reverse_index):
            print(token)
            if index == 100:
                break

        self.dl = IndexFiles.load(IndexFiles.DOC_LENGTH)
        if os.path.exists(IndexFiles.DOC_FREQS):
            self.df = IndexFiles.load(IndexFiles.DOC_FREQS)
        else:
            self.create_df(IndexFiles.DOC_FREQS)

    def search(self, query, num, verbose=True, with_scores=False):
        stemmed_query = self.stemmer.process(query).split()

        self.bm25 = Counter()
        for token in stemmed_query:
            self.update_bm25(token)

        most_wanted = [(uid, rank) for (uid, rank) in self.bm25.most_common(num)]

        if verbose:
            for (uid, rank) in most_wanted:
                print('rank: {0:3.3f} https://vk.com/id{1}'.format(rank, uid))

        if with_scores:
            return most_wanted
        else:
            return [uid for (uid, _) in most_wanted]

    def update_bm25(self, token):
        if token in self.reverse_index:
            for doc in self.reverse_index[token]:
                tf = self.reverse_index[token][doc]
                bm25_token = math.log(self.N / self.df[token]) * ((self.k1 + 1) * tf) / \
                             (self.k1 * ((1 - self.b) + self.b * (self.dl[doc] / self.d_avg)) + tf)
                self.bm25[doc] += bm25_token

    def create_df(self, filename):

        print('Building doc frequencies:')

        self.df = Counter()

        for token in tqdm.tqdm(self.reverse_index, total=len(self.reverse_index)):
            for uid in self.reverse_index[token]:
                self.df[token] += self.reverse_index[token][uid]

        IndexFiles.dump(filename, self.df)


class Doc2vecSearcher:
    def __init__(self):
        self.model = Doc2Vec.load('forward_index.doc2vec')
        self.s = Stemmer()

    def search(self, query, n_results, verbose=True):
        new_vector = self.model.infer_vector(query)
        most_wanted = self.model.docvecs.most_similar([new_vector], topn=n_results)

        if verbose:
            for (uid, rank) in most_wanted:
                print('rank: {0:3.3f} https://vk.com/id{1}'.format(rank, uid))

        return [uid for (uid, _) in most_wanted]


class FeedBack:
    def __init__(self, mode, query, ids, filename):
        self.mode = mode
        self.query = query
        self.ids = ids
        self.filename = filename

    def get_feedback(self):
        cont = []

        if os.path.exists(self.filename):
            with open(self.filename, 'rb') as handle:
                cont = pickle.load(handle)

        feedback = input('assign to each search result you want a score from 0 to 5 like this: '
                         '1-0 2-5 7-3 and hit enter: ')

        feedback = feedback.split()
        feedback = [feed.split('-') for feed in feedback]

        print(feedback)

        mode_literal = 's' if self.mode == 'search' else 'r'

        for (index, score) in feedback:
            cont.append((mode_literal, self.query, self.ids[int(index)], int(score)))

        with open(self.filename, 'wb') as handle:
            pickle.dump(cont, handle)


def main(args):
    if args.method == 'BM25':
        searcher = BM25()
    elif args.method == 'doc2vec':
        searcher = Doc2vecSearcher()
    else:
        raise ValueError('unknown method. Only BM25 and doc2vec are supported')

    n_results = args.n_results

    if args.mode == 'recommend':
        id = int(input('please give us your vk id '))
        query = searcher.stemming(download_users.get_user_info(id))
    else:
        # query = searcher.stemming(args.query)
        query = args.query

    while True:
        ids = searcher.search(query, n_results)

        feedback = input('do you wish to leave feedback? ')

        if feedback == 'Yes' or feedback == 'yes':
            if args.mode == 'search':
                feed = FeedBack(args.mode, query, ids, 'feedback.pickle')
            elif args.mode == 'recommendation':
                feed = FeedBack(args.mode, id, ids, 'feedback.pickle')
            feed.get_feedback()

        change_number = input('do you wish to change number of results? ')

        if change_number == 'Yes' or change_number == 'yes':
            n_results = int(input('give a new number of results: '))
            continue

        go_on = input('do you wish to continue? ')

        if go_on == 'Yes' or go_on == 'yes':
            query = input('give a new query: ')
            continue
        break


class SearchEngine:
    def __init__(self, db):
        self.bm25 = None
        self.doc2vec_searcher = None
        self.db = db

    def search(self, method, mode, query, max_num_of_results, gender, age_range, city):
        if method == 'BM25':
            if not self.bm25:
                self.bm25 = BM25()
            searcher = self.bm25
        elif method == 'doc2vec':
            if not self.doc2vec_searcher:
                self.doc2vec_searcher = Doc2vecSearcher()
            searcher = self.doc2vec_searcher
        else:
            raise ValueError('unknown method. Only BM25 and doc2vec are supported')

        if mode == 'recommend':
            query_processed = searcher.stemming(download_users.get_user_info(query))
        else:
            query_processed = searcher.stemming(query)

        # satisfactory_uids = [user['uid'] for user in
        #                      self.db.users.find({
        #                         '$and': [
        #                             {'gender': gender}, {'city': city},
        #                             {'age': {'$gte': age_range[0], '$lte': age_range[1]}}
        #                         ]
        #                     })]

        while True:
            ids = searcher.search(query_processed, max_num_of_results, verbose=False, with_scores=True)

            # filtered_ids = [uid for uid in ids if uid in satisfactory_uids]
            filtered_ids = ids

            if len(filtered_ids) == max_num_of_results:
                return filtered_ids
            elif max_num_of_results >= len(satisfactory_uids):
                return filtered_ids

            max_num_of_results *= 4


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='My purpose is to pass a butter')
    parser.add_argument('--method', type=str, default='BM25',
                        help='BM25 or doc2vec')
    parser.add_argument('--mode', type=str, default='search',
                        help='search or recommend')
    parser.add_argument('--query', type=str, default='',
                        help='Query. If more than one word should be inside parenthesis')
    parser.add_argument('--n_results', type=int, default=10,
                        help='Number of results to show')

    args = parser.parse_args()

    main(args)
