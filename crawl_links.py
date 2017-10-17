import time
import argparse

from urllib import robotparser

from pymongo import MongoClient

def get_links_set(db):
    links = set()

    for l in db.links.find():
        links.update(l['links'])

    return links

def main(args):
    client = MongoClient()
    db = client.ir_project

    links_list = get_links_set(db)

    print(len(links_list))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Script to crawl external links from users')

    args = parser.parse_args()

    main(args)
