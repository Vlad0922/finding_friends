import download_users
import time
import argparse
import pickle
import os
import math
import tqdm

from gensim.models import Doc2Vec
from collections import Counter

from utils import IndexFiles, Stemmer, Timer


class BM25:
    def __init__(self):
        self.reverse_index = None
        self.doc_lengths = None
        self.token_freqs = None
        self.user_infos = None
        self.bm25 = None
        self.k1 = 1.5
        self.b = 0.75
        self.stemmer = Stemmer()
        self.load_data()
        self.N = len(self.doc_lengths)
        self.avg_length = sum(list(self.doc_lengths.values())) / self.N

    def load_data(self):
        with Timer('Loading bm25 files'):
            self.reverse_index = IndexFiles.load(IndexFiles.REVERSE_INDEX)
            self.doc_lengths = IndexFiles.load(IndexFiles.DOC_LENGTH)
            self.token_freqs = IndexFiles.load(IndexFiles.DOC_FREQS)
            self.user_infos = IndexFiles.load(IndexFiles.USER_INFOS)

    def search(self, query, num, gender, city, age_from, age_to, verbose=True, with_scores=False):
        stemmed_query = self.stemmer.process(query).split()

        self.bm25 = Counter()
        for token in stemmed_query:
            self.update_bm25(token)

        for uid in self.user_infos:
            if self.user_infos[uid]['sex'] != gender or \
                    self.user_infos[uid]['city'] != city or \
                    self.user_infos[uid]['age'] < age_from or \
                    self.user_infos[uid]['age'] > age_to:

                del self.bm25[uid]

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
                bm25_token = math.log(self.N / self.token_freqs[token]) * ((self.k1 + 1) * tf) / \
                             (self.k1 * ((1 - self.b) + self.b * (self.doc_lengths[doc] / self.avg_length)) + tf)
                self.bm25[doc] += bm25_token


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


class SearchEngine:
    def __init__(self):
        self.bm25 = None
        self.doc2vec_searcher = None

    def search(self, method, mode, query, max_num_of_results, gender, city, age_from, age_to):
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

        with Timer('Searching for {} users'.format(max_num_of_results)):
            uids = searcher.search(query, max_num_of_results, gender, city,
                                   age_from, age_to, verbose=True, with_scores=True)

        return uids


def main(args):
    if args.method != 'BM25' and args.method != 'doc2vec':
        raise ValueError('unknown method. Only BM25 and doc2vec are supported')

    searcher = SearchEngine()

    if args.mode == 'recommend':
        uid = int(input('please give us your vk id '))
        query = download_users.get_user_info(uid)
    else:
        query = args.query

    print(query)

    while True:
        uids = searcher.search(args.method, args.mode, query,
                               args.n_results, args.gender,
                               args.city, args.age_from, args.age_to)

        feedback = input('do you wish to leave feedback? ')

        if feedback.lower() == 'yes':
            if args.mode == 'search':
                feed = FeedBack(args.mode, query, uids, 'feedback.pickle')
            elif args.mode == 'recommendation':
                feed = FeedBack(args.mode, id, uids, 'feedback.pickle')
            feed.get_feedback()

        change_number = input('do you wish to change number of results? ')

        if change_number.lower() == 'yes':
            n_results = int(input('give a new number of results: '))
            continue

        go_on = input('do you wish to continue? ')

        if go_on.lower() == 'yes':
            query = input('give a new query: ')
            continue
        break


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
    parser.add_argument('--gender', type=int, default=1,
                        help='Filter by gender')
    parser.add_argument('--city', type=int, default=2,
                        help='Filter by city')
    parser.add_argument('--age_from', type=int, default=18,
                        help='Age lower bound')
    parser.add_argument('--age_to', type=int, default=30,
                        help='Age upper bound')

    arguments = parser.parse_args()

    main(arguments)
