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
    batch_size = search_params['count']
    offset = 1

    result = api.users.search(**search_params)  # first value in list is total size of peoples in query result
    total_count = result[0]

    db.users.insert_many(result[1:])

    print('Loading users...')
    with tqdm.tqdm(total=total_count, initial=batch_size) as pbar:
        while offset*batch_size < total_count:
            time.sleep(3)  # internal vk delay for search query

            result = api.users.search(**search_params, offset=offset)
            db.users.insert_many(result[1:])

            offset += 1
            pbar.update(batch_size)


def get_wall_params():
    params = dict()

    params['filter'] = 'owner' # only owner posts to get only owners views
    params['extended'] = 0 # we don't need this extra information
    params['count'] = 10 # 10 only for testing

    return params


def load_walls(api):
    client = MongoClient()
    db = client.ir_project

    wall_params = get_wall_params()
    batch_size = wall_params['count']

    for user in db.users.find():
        posts = get_all_user_posts(api, user)
        print(api.wall.get(**wall_params, owner_id=user['uid']))
        break

def main():
    auth_params = get_auth_params()
    session = vk.AuthSession(**auth_params)

    api = vk.API(session)
    # load_users(api)
    load_walls(api)



if __name__ == '__main__':
    main()
