import time
import argparse

import vk

import tqdm

from pymongo import MongoClient


AGE_FROM = 16  # russian law is strict
AGE_TO = 80    # well...


def get_search_params():
    params = dict()

    params['sex'] = 1         # only girls!
    params['status'] = 6      # only in active search
    params['city'] = 2        # SpB is our default city ;)
    params['country'] = 1     # only russian girls
    params['has_photo'] = 1   # to filter fakes or empty pages
    params['count'] = 1000    # 10 only for testing

    return params


def get_auth_params(config_fname='auth_data.txt'):
    params = dict()

    with open(config_fname) as in_file:
        params['app_id'] = in_file.readline().strip()
        params['user_login'] = in_file.readline().strip()
        params['user_password'] = in_file.readline().strip()

    return params


def load_users(api):
    client = MongoClient()
    db = client.ir_project

    search_params = get_search_params()

    for i in tqdm.trange(AGE_FROM, AGE_TO+1, desc='Loading users...'):  # we cannot have more than 1000 results in one query so let's split
        search_params['age_from'] = i                                   # one big query in multiple smaller
        search_params['age_to'] = i

        result = api.users.search(**search_params)  # first value in list is total size of peoples in query result

        if result[0] != 0:
            db.users.insert_many(result[1:])

        time.sleep(0.4)  # internal cooldown for vk


def get_wall_params():
    params = dict()

    params['filter'] = 'owner'  # only owner posts to get only owners views
    params['extended'] = 0      # we don't need this extra information
    params['count'] = 100       # 10 only for testing

    return params


def load_wall_posts(api):
    client = MongoClient()
    db = client.ir_project

    wall_params = get_wall_params()
    total_users = db.users.count()

    for user in tqdm.tqdm(db.users.find(), total=total_users, desc='Loading wall posts...'):
        try:
            result = api.wall.get(**wall_params, owner_id=user['uid'])

            if result[0] != 0:
                db.wall_posts.insert_many(result[1:])
        except Exception as e:
            print(e)

        time.sleep(0.4)  # internal cooldown for vk: not more than 3 requests per second


def get_user_params():
    params = dict()

    params['fields'] = 'photo_max_orig, education, personal, interests,activities,music, movies, tv, books, games, about, quotes, schools, site, occupation'

    return params


def get_user_ids(db, f, t):
    return ','.join([str(user['uid']) for user in db.users.find()[f:t]])

def load_user_info(api):
    client = MongoClient()
    db = client.ir_project

    user_params = get_user_params()
    total_users = db.users.count()

    batch_size = 1000

    for i in tqdm.trange(0, total_users, batch_size, desc='Loading users info...'):
        users = get_user_ids(db, i, i+batch_size)
        try:
            result = api.users.get(**user_params, user_ids=users)

            if len(result) != 0:
                db.user_info.insert_many(result)
        except Exception as e:
            print(e)

        time.sleep(0.4)


def main(args):
    auth_params = get_auth_params()
    session = vk.AuthSession(**auth_params)

    api = vk.API(session)

    if args.users:
        load_users(api)

    if args.wall_posts:
        load_wall_posts(api)

    if args.user_info:
        load_user_info(api)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Script to download users and information about them')

    parser.add_argument('--users', action='store_true', default=False,
                        help='Download users?')
    parser.add_argument('--wall_posts', action='store_true', default=False,
                        help='Download wall posts for users in db?')
    parser.add_argument('--user_info', action='store_true', default=False,
                        help='Download user info?')

    args = parser.parse_args()

    main(args)
