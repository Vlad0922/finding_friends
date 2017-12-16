import pickle
import json
from pymorphy2 import MorphAnalyzer
from nltk.corpus import stopwords
from nltk.tokenize import RegexpTokenizer
from os import listdir
from os.path import isfile, join


class IndexFiles:
    TMP_DIR = './tmp/'
    RAW_PAT = 'forward_index_raw_{:03}.json'
    FORWARD_INDEX = 'forward_index.json'
    REVERSE_INDEX = 'reverse_index.json'
    DOC_LENGTH = 'doc_length.json'
    DOC_FREQS = 'doc_freqs.json'

    @staticmethod
    def load(filename):
        extension = filename.split('.')[-1]
        if extension == 'pickle':
            return IndexFiles.load_pickle(filename)
        elif extension == 'json':
            return IndexFiles.load_json(filename)
        else:
            raise ValueError('Unsupported type for loading')

    @staticmethod
    def dump(filename, obj):
        extension = filename.split('.')[-1]
        if extension == 'pickle':
            IndexFiles.dump_pickle(filename, obj)
        elif extension == 'json':
            IndexFiles.dump_json(filename, obj)
        else:
            raise ValueError('Unsupported type for dumping')

    @staticmethod
    def load_pickle(filename):
        with open(filename, 'rb') as handle:
            return pickle.load(handle)

    @staticmethod
    def load_json(filename):
        with open(filename, 'r') as handle:
            return json.load(handle)

    @staticmethod
    def dump_pickle(filename, obj):
        with open(filename, 'wb') as handle:
            pickle.dump(obj, handle)

    @staticmethod
    def dump_json(filename, obj):
        with open(filename, 'w') as handle:
            json.dump(obj, handle, ensure_ascii=False)

    @staticmethod
    def clean_tmp(directory):
        [os.remove(join(directory, f))
         for f in listdir(directory)
         if isfile(join(directory, f))]


class Stemmer:
    def __init__(self):
        self.morpher = MorphAnalyzer()
        russian_stopwords = set(stopwords.words('russian'))
        english_stopwords = set(stopwords.words('english'))
        custom_stops = {'br', 'ask', 'fm', 'http', 'https', 'www', 'ru', 'com', 'vk', 'view',
                        'vkontakte', 'd1', 'd0', 'amp', 'utm_source',  'utm_medium', 'utm_campaign'}

        self.stops = russian_stopwords | english_stopwords | custom_stops | self.custom_stops()
        self.tokenizer = RegexpTokenizer(r'\w+')
        self.cache = dict()

    @staticmethod
    def custom_stops():
        res = set()
        with open('stops.txt') as in_file:
            for line in in_file:
                res.add(line.strip())
        return res

    def is_valid(self, w):
        return not(w in self.stops
                   or not w.isalpha()
                   or w.startswith('id')
                   or w.startswith('club')
                   or w.startswith('app')
                   or set(w) == {'_'})

    def get_normal_form(self, word):
        if word not in self.cache:
            self.cache[word] = self.morpher.parse(word)[0].normal_form
        return self.cache[word]

    def process(self, text):
        words = [w for w in self.tokenizer.tokenize(text.lower())]
        words = [word for word in words if self.is_valid(word)]
        words = [self.get_normal_form(w) for w in words]
        words = [word for word in words if self.is_valid(word)]
        return ' '.join(words)

