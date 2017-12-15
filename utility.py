from nltk.corpus import stopwords
from nltk.tokenize import RegexpTokenizer

from pymorphy2 import MorphAnalyzer

import pickle

from sklearn.feature_extraction.text import CountVectorizer
from sklearn.decomposition import LatentDirichletAllocation
from pymystem3 import Mystem

def read_stops():
    res = set()
    with open('stops.txt') as in_file:
        for line in in_file:
            res.add(line.strip())
    return res

def is_number(s):
    try:
        float(s)
        return True
    except:
        return False

class Stemmizer(object):
    def __init__(self):
        self.morpher = MorphAnalyzer()

        russian_stopwords = set(stopwords.words('russian'))
        english_stopwords = set(stopwords.words('english'))
        custom_stops = {'br', 'ask', 'fm', 'http', 'https', 'www', 'ru', 'com', 'vk', 'view',
                        'vkontakte', 'd1', 'd0', 'amp', 'utm_source',  'utm_medium', 'utm_campaign'}

        self.stops = russian_stopwords | english_stopwords | custom_stops | read_stops()

        self.tokenizer = RegexpTokenizer(r'\w+')
        self.stemmer = Mystem()

    def _is_valid(self, w):
        return not(w.startswith('id') or is_number(w) or w in self.stops
                   or w.startswith('club') or w.startswith('app') or set(w) == {'_'})

    def process(self, text):
        words = [w for w in self.tokenizer.tokenize(text.lower())]
        words = [word for word in words if self._is_valid(word)]

#        text  = ' '.join([w for w in self.tokenizer.tokenize(text.lower())])
#        words = self.stemmer.lemmatize(text)
#        words = self.morpher.normal_forms(text)

        words = [self.morpher.parse(w)[0].normal_form for w in words]

#        words = [w for w in self.tokenizer.tokenize(text.lower())]
#        words = [self.morpher.parse(w)[0].normal_form for w in words]

        words = [word for word in words if self._is_valid(word)]

        return ' '.join(words)
