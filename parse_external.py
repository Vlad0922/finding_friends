import time
import argparse
import re

from collections import defaultdict

import tldextract

import tqdm

from pymongo import MongoClient


# I want to create regex object just once
regex_ = re.compile(
    r'^https?://'  # http:// or https://
    r'(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+[A-Z]{2,6}\.?|'  # domain...
    r'localhost|'  # localhost...
    r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})'  # ...or ip
    r'(?::\d+)?'  # optional port
    r'(?:/?|[/?]\S+)$', re.IGNORECASE)


def is_valid_url(url):
    return url is not None and regex_.search(url)


bad_domains = {
                'vk', 'vkontakte',   # crossreferences
                'nlstar',            # spam
                }


def check_domain(url):
    d = tldextract.extract(url).domain
    return not (d in bad_domains)


def find_urls(text):
    urls = re.findall('http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\(\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+', text)
    return [u for u in urls if is_valid_url(u) and check_domain(u)]


def check_personal(user):
    lst = list()

    if 'site' in user:
        urls = find_urls(user['site'])
        lst.extend(urls)

    if 'about' in user:
        urls = find_urls(user['about'])
        lst.extend(urls)

    return lst


def create_links_info(db):
    links = dict()
    for user in db.user_info.find():
        user_links = check_personal(user)

        if len(user_links) > 0:
            links[user['uid']] = user_links

    return links


def create_links_wposts(db):
    links = defaultdict(lambda: list())

    for p in tqdm.tqdm(db.wall_posts.find(), total=db.wall_posts.count(), desc='Wall posts'):
        text_urls = find_urls(p['text'])
        post_urls = list()

        if 'attachments' in p:
            for a in p['attachments']:
                if a['type'] == 'link':
                    a_url = a['link']['url']
                    if check_domain(a_url):
                        post_urls.append(a_url)

        if len(text_urls) > 0 or len(post_urls) > 0:
            links[p['from_id']].extend(text_urls + post_urls)

    return links


def main(args):
    client = MongoClient()
    db = client.ir_project

    all_links = defaultdict(lambda: set())

    if args.user_info:
        site_links = create_links_info(db)

        for k in site_links:
            all_links[k].update(site_links[k])

    if args.wall_posts:
        wall_links = create_links_wposts(db)

        for k in wall_links:
            all_links[k].update(wall_links[k])

    all_links = [{'uid': k, 'links': list(all_links[k])} for k in all_links]  # prepare links for mongo

    if len(all_links) > 0:
        db.links.insert_many(all_links)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Script to crawl external links from users')
    parser.add_argument('--user_info', action='store_true', default=False,
                        help='Parse users info?')
    parser.add_argument('--wall_posts', action='store_true', default=False,
                        help='Parse wall posts?')

    args = parser.parse_args()

    main(args)
