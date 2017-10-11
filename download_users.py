import time

import vk

import tqdm

from pymongo import MongoClient


def get_search_params():
    params = dict()

    params['sex'] = 1        # only girls!
    params['status'] = 6     # only in active search
    params['age_from'] = 16  # russian law is strict
    params['city'] = 2       # SpB is our default city ;)
    params['country'] = 1    # only russian girls
    params['has_photo'] = 1  # to filter fakes or empty pages
    params['count'] = 100    # 10 only for testing

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
    offset = search_params['count']
    batch_size = search_params['count']

    result = api.users.search(**search_params)  # first value in list is total size of peoples in query result
    total_count = result[0]

    db.users.insert_many(result[1:])

    print('Loading users...')
    with tqdm.tqdm(total=total_count, initial=offset) as pbar:
        while offset < total_count:
            time.sleep(3)  # internal vk delay for search query

            result = api.users.search(**search_params, offset=offset)
            db.users.insert_many(result[1:])

            offset += batch_size
            pbar.update(batch_size)


def main():
    auth_params = get_auth_params()
    session = vk.AuthSession(**auth_params)

    api = vk.API(session)
    load_users(api)

if __name__ == '__main__':
    main()
