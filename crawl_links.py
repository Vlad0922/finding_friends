import time
import argparse

from collections import defaultdict

# from queue import Queue

import urllib.request
from urllib.error import HTTPError
from urllib import robotparser

import tldextract

import tqdm

from gridfs import GridFS
from pymongo import MongoClient

from multiprocessing.dummy import Pool
from threading import Lock

from bs4 import BeautifulSoup


def get_thumbnail_url(youtube_id, max_res=False):
    if max_res:
        return 'https://img.youtube.com/vi/{}/maxresdefault.jpg'.format(youtube_id)
    else:
        return 'https://img.youtube.com/vi/{}/default.jpg'.format(youtube_id)


def get_links_set(db, only_new=True):
    links = set()

    for link_info in db.links.find():
        for l in link_info['links']:
            links.update(l.split('<br>'))

    if only_new:
        worked_links = set()
        worked_links.update([link_info['url'] for link_info in db.links_content.find()])
        worked_links.update([link_info['url'] for link_info in db.links_failed.find()])

        links = links - worked_links

    return links    


class Crawler(object):
    def __init__(self, db, n_threads=10, max_len=500):
        self.db = db
        self.links = get_links_set(db)

        # import random
        # self.links = random.sample(self.links, 100) # for testing purposes

        self.n_threads = n_threads
        self.mutex = Lock()

        self.result = list()
        self.failed = list()

        self.max_list_len = max_len

        self._create_domains_map()

    def _create_domains_map(self):
        self.domain_func = defaultdict(lambda: self._load_common)

        self.domain_func['youtube'] = self._load_youtube
        self.domain_func['youtu'] = self._load_youtube
        self.domain_func['ask'] = self._load_ask
        self.domain_func['ali'] = self._load_ali
        self.domain_func['livejournal'] = self._load_lj
        self.domain_func['pikabu'] = self._load_pikabu
        self.domain_func['sprashivai'] = self._load_sprashivai

    def _load_page(self, url):
        d = tldextract.extract(url).domain
        page = urllib.request.urlopen(url, timeout=5)
        content = page.read().decode(page.headers.get_content_charset())
        soup = BeautifulSoup(content, 'lxml')

        try:
            return self.domain_func[d](soup)
        except:
            return self._load_common(soup)


    def start(self):
        self.pbar = tqdm.tqdm(total = len(self.links))

        pool = Pool(processes=self.n_threads)
        pool.map(self._links_worker, self.links)

        if len(self.result):
            self.db.links_content.insert_many(self.result)
            self.result = list()

        # if len(self.failed):
        #     self.db.links_content.insert_many(self.failed)
        #     self.failed = list()


    def _links_worker(self, url):
        def _do_step(content, status):
            self.mutex.acquire()

            content['url'] = url

            if status == 'ok':
                self.result.append(content)
            # else:
                # self.failed.append(content)

            if len(self.result) > self.max_list_len:
                self.db.links_content.insert_many(self.result)
                self.result = list()

            # if len(self.failed) > self.max_list_len:
            #     self.db.links_failed.insert_many(self.failed)
            #     self.failed = list()

            self.pbar.update(1)

            self.mutex.release()

        page_content = {}
        try:
            page_content = self._load_page(url)
        except urllib.error.HTTPError as e:
            status = "HTTP_error"
        except Exception as e:
            status = "unknown_error"
        else:
            status = "ok"
            time.sleep(0.25)

        # _inc_pbar()
        _do_step(page_content, status)

        # return (page_content, status)


    def _load_sprashivai(soup):
        answers = [ans.text for ans in soup.find_all(class_='text_answer')]

        return {
                'type': 'sprashivai',
                'answers': answers
                }

    def _load_lj(self, soup):
        text = soup.find(class_='b-singlepost-bodywrapper').text
        title = soup.find(property='og:title').get('content')

        return {
                'type': 'livejournal',
                'text': text,
                'title': title
                }


    def _load_pikabu(soup):
        text = soup.find(class_='b-story-block__content').text.strip()
        title = soup.find(property='og:title').get('content')

        return {
                'type': 'pikabu',
                'text': text,
                'title': title
                }

    def _load_youtube(self, soup):
        desc = soup.find(id="eow-description").text
        tags = [d.get('content') for d in soup.findAll(property='og:video:tag')]
        name = soup.find(id="eow-title").text.strip()

        return {
                'type': 'youtube',
                'description': desc,
                'tags': tags,
                'name': name}


    def _load_ali(self, soup):
        name = soup.find(property="og:title").get('content')        

        return {
                'type': 'ali',
                'name': name
                }

    def _load_ask(self, soup):
        ans = [d.text.split()[0] for d in soup.findAll(class_="streamItem_content")]      

        return {
                'type': 'ask',
                'answers': ans
                }

    def _load_common(self, soup):
        title = soup.title.string

        try:
            desc = soup.find(property='og:description').get('content')
        except:
            desc = str()

        return {
                'type': 'unknown',
                'title': title,
                'description': desc
                }


def main(args):
    client = MongoClient()
    db = client.ir_project

    crawler = Crawler(db, args.n_threads)
    crawler.start()

    print(len(crawler.result), len(crawler.failed))

    # db.links_content.insert_many(crawler.result)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Script to crawl external links from users')
    parser.add_argument('--n_threads', type=int, default=10,
                        help='Crawler threads count')
    
    args = parser.parse_args()

    main(args)
