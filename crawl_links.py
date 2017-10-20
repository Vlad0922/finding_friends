import time
import argparse

from collections import defaultdict

from queue import Queue
import urllib.request
from urllib import robotparser

import tldextract

from gridfs import GridFS
from pymongo import MongoClient

from threading import Thread

from bs4 import BeautifulSoup


def get_thumbnail_url(youtube_id, max_res=False):
    if max_res:
        return 'https://img.youtube.com/vi/{}/maxresdefault.jpg'.format(youtube_id)
    else:
        return 'https://img.youtube.com/vi/{}/default.jpg'.format(youtube_id)

class Crawler(object):
    def __init__(self, links, n_threads=10):
        self.links = Queue()

        for l in links:
            self.links.put(l)

        self.result = Queue()
        self.failed = Queue()

        self.n_threads = 10

        self._create_domains_map()

    def _create_domains_map(self):
        self.domain_func = defaultdict(lambda: self._load_common)

        self.domain_func['youtube'] = self._load_youtube
        self.domain_func['youtu'] = self._load_youtube
        self.domain_func['ask'] = self._load_ask
        self.domain_func['ali'] = self._load_ali

    def _load_page(self, url):
        d = tldextract.extract(url).domain

        return self.domain_func[d](url)


    def _links_worker(self):
        while not self.links.empty():
            url = self.links.get()

            try:
                page_content = self._load_page(url)
                self.result.put(page_content)
            except Exception as e:
                self.failed.put((url, e))

    def _load_youtube(self, url):
        page = urllib.request.urlopen(url)
        content = page.read().decode(page.headers.get_content_charset())

        soup = BeautifulSoup(content, 'lxml')
        desc = soup.find(id="eow-description").text
        tags = [d.get('content') for d in soup.findAll(property='og:video:tag')]
        name = soup.find(id="eow-title").text.strip()

        return {'type': 'youtube',
                'description': desc,
                'tags': tags,
                'name': name}


    def _load_ali(self, url):
        page = urllib.request.urlopen(url)
        content = page.read().decode(page.headers.get_content_charset())

        soup = BeautifulSoup(content, 'lxml')
        name = soup.find(property="og:title").get('content')        

        return {
                'type': 'ali',
                'name': name
                }

    def _load_ask(self, url):
        page = urllib.request.urlopen(url)
        content = page.read().decode(page.headers.get_content_charset())

        soup = BeautifulSoup(content, 'lxml')
        ans = [d.text.split()[0] for d in soup.findAll(class_="streamItem_content")]      

        return {
                'type': 'ask',
                'answers': ans
                }

    def _load_common(self, url):
        page = urllib.request.urlopen(url)
        content = page.read().decode(page.headers.get_content_charset())

        soup = BeautifulSoup(content, 'lxml')
        title = soup.title.string
        return {
                'type': 'unknown',
                'title': title
                }


    def start(self):
        threads = list()
        for _ in range(self.n_threads):
            t = Thread(target = self._links_worker)
            t.start()
            threads.append(t)

        for t in threads:
            t.join()


def get_links_set(db):
    links = set()

    for l in db.links.find():
        links.update(l['links'])

    return links


def main(args):
    client = MongoClient()
    db = client.ir_project

    links_list = get_links_set(db)

    crawler = Crawler(links_list)
    crawler.start()

    print(crawler.result.qsize())

    db.links_content.insert_many(crawler.result)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Script to crawl external links from users')

    args = parser.parse_args()

    main(args)
