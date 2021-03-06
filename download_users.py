import time
import argparse

import itertools

import vk

import tqdm

from pymongo import MongoClient


AGE_FROM = 18  # russian law is strict
AGE_TO = 40    # well...


fields_to_filter = ['comments', 'from_id', 'id', 'is_pinned', 'likes', 'online', 'post_source', 'post_type', 'reply_count', 'reposts', 'to_id']
fields_to_left = ['text']
attach_fields = ['text', 'title', 'description']


def get_user_info(uid):
    return ''


def dictproduct(dct):
    for t in itertools.product(*dct.values()):
        yield dict(zip(dct.keys(), t))


def get_search_params():
    params = dict()

    params['sex'] = 1         # only girls!
    params['status'] = 6      # only in active search
    params['city'] = 1        # SpB is our default city ;)
    params['country'] = 1     # only russian girls
    params['has_photo'] = 1   # to filter fakes or empty pages
    params['count'] = 1000    # 10 only for testing
    params['fields'] = 'bdate,sex,relation,city,country'

    return params


def get_auth_params(config_fname='auth_data.txt'):
    params = dict()

    with open(config_fname) as in_file:
        params['app_id'] = in_file.readline().strip()
        params['user_login'] = in_file.readline().strip()
        params['user_password'] = in_file.readline().strip()

    return params


def get_iterate_params():
    params = dict()

    params['sex'] = [1, 2]
    params['city'] = [1 , 2]
    params['status'] = list(range(1, 8))
    params['city'] = [1, 2, 169]

    return params


def load_users(api):
    client = MongoClient()
    db = client.ir_project

    search_params = get_search_params()
    search_combs = list(dictproduct(get_iterate_params()))

    for i in tqdm.trange(AGE_FROM, AGE_TO+1, desc='Loading users...'):  # we cannot have more than 1000 results in one query so let's split     
        search_params['age_from'] = i                                   # one big query in multiple smaller
        search_params['age_to'] = i

        age_total = 0
        for comb in search_combs:
            for k in comb.keys():
                search_params[k] = comb[k]

            result = api.users.search(**search_params)  # first value in list is total size of peoples in query result
            age_total += result[0]

            # print(result[0])

            if result[0] != 0:
                for r in result[1:]:
                    r['age'] = i

                db.users.insert_many(result[1:])

            time.sleep(0.4)  # not more than 3 queries per second
        print('{} total: {}'.format(i, age_total))


def try_get(dct, field):
    if field in dct:
        return dct[field]
    else:
        return ''


def filter_wall_query(query):
    if type(query) is list:
        res = query[1:]

        def filter_single(q):
            filt = dict()
                
            def filter_attach(attach):
                res = dict()
                
                if 'video' in attach:
                    for f in attach_fields:
                        res[f] = try_get(attach['video'], f)
                    
                return res

            for f in fields_to_left:
                filt[f] = try_get(q, f) 
                
            if 'attachments' in q:
                filt['attachments'] = list()
                for a in q['attachments']:
                    filt['attachments'].append(filter_attach(a))

            return filt

        return [filter_single(q) for q in res]
    else:
        return []

def filter_subscriptions_query(query):
    if query == False:
        return []
    
    def filter_single(q):
        return {'name':q['name'], 'gid':q['gid']}

    return [filter_single(q) for q in query if q['type'] == 'page']

def filter_groups_query(query):
    if query == False:
        return []
    
    def filter_single(q):
        return {'name':q['name'], 'gid':q['gid']}

    return [filter_single(q) for q in query[1:] if q['type'] == 'page']


def get_wall_params():
    params = dict()

    params['filter'] = 'owner'  # only owner posts to get only owners views
    params['extended'] = 0      # we don't need this extra information
    params['count'] = 100       # 10 only for testing

    return params

def get_subscript_params():
    params = dict()

    params['extended'] = 1

    return params

def get_groups_params():
    params = dict()

    params['extended'] = 1

    return params


def create_execute_code(params_orig, id_list, method, user_field='owner_id'):
    params = list(params_orig.items())

    def create_single(idx):
        c = 'API.{}({{'.format(method)
        for p, v in (params + [(user_field, idx)]):
            c += '"{}" : "{}",'.format(p, v)
        c += '}),'

        return c

    code = 'return[';
    for idx in id_list:
        code += create_single(idx)
    code += '];'

    return code


def load_user_text(uid):
    print('Loading text for user: {}'.format(uid))
    sess =  vk.Session(access_token='384caffdac72438ecf840f594ce7c59a0a4976332a5e309c50f55d1a8fe46de70529902f0bec9859e6781')
    api = vk.API(sess)

    wall_params = get_wall_params()
    wall_params['owner_id'] = uid
    g_params = get_groups_params()
    g_params['uid'] = uid
    user_params = get_user_params()

    wall_posts = api.wall.get(**wall_params)[1:]
    subscrs = api.users.getSubscriptions(**g_params)[1:]
    groups = api.groups.get(**g_params)[1:]


    text = ''

    for p in wall_posts:
        text += ' ' + p['text']

    for s in subscrs:
        text += ' ' + s['name']
    
    for g in groups:
        text += ' ' + g['name']

    return text 


def load_subsciptions(api):
    client = MongoClient()
    db = client.ir_project

    subscript_params = get_subscript_params()
    all_ids = set([u['uid'] for u in db.users.find()])
    # all_ids = set([u['uid'] for u in db.users.find({'sex': 1, 'age': {'$lte': 30}})])
    downloaded_ids = set([u['uid'] for u in db.subscriptions.find()])

    current_ids = list(all_ids - downloaded_ids)
    batch_size = 25


    for i in tqdm.trange(0, len(current_ids), batch_size, desc='Loading subscriptions...'):
        users = current_ids[i:i+batch_size]
        try:
            code = create_execute_code(subscript_params, id_list=users, method='users.getSubscriptions', user_field='user_id')
            result = api.execute(code=code, timeout=60)

            if all([r == False for r in result]):
                print('empty query...')
                empty_counter += 1
            else:   
                filtered = [filter_subscriptions_query(q) for q in result]
                db_query = [{'uid': idx, 'subscriptions': p} for (idx, p) in zip(users, filtered)]

                db.subscriptions.insert_many(db_query)

                empty_counter = 0

            if empty_counter >= 3:
                break
                    
        except Exception as e:
            print(e)

        time.sleep(0.4)


def load_groups(api):
    client = MongoClient()
    db = client.ir_project

    subscript_params = get_groups_params()
    all_ids = set([u['uid'] for u in db.users.find()])
    # all_ids = set([u['uid'] for u in db.users.find({'sex': 1, 'age': {'$lte': 30}})])
    downloaded_ids = set([u['uid'] for u in db.groups.find()])

    current_ids = list(all_ids - downloaded_ids)
    batch_size = 25


    for i in tqdm.trange(0, len(current_ids), batch_size, desc='Loading subscriptions...'):
        users = current_ids[i:i+batch_size]
        try:
            code = create_execute_code(subscript_params, id_list=users, method='groups.get', user_field='user_id')
            result = api.execute(code=code, timeout=60)

            if all([r == False for r in result]):
                print('empty query...')
                empty_counter += 1
            else:   
                filtered = [filter_groups_query(q) for q in result]
                db_query = [{'uid': idx, 'subscriptions': p} for (idx, p) in zip(users, filtered)]

                db.groups.insert_many(db_query)

                empty_counter = 0

            if empty_counter >= 3:
                break
                    
        except Exception as e:
            print(e)

        time.sleep(0.4)


def load_wall_posts(api):
    client = MongoClient()
    db = client.ir_project

    wall_params = get_wall_params()
    all_ids = set([u['uid'] for u in db.users.find({'sex': 1, 'age': {'$lte': 30}})])
    downloaded_ids = set([u['uid'] for u in db.wall_posts.find()])

    current_ids = list(all_ids - downloaded_ids)
    batch_size = 25

    empty_counter = 0

    for i in tqdm.trange(0, len(current_ids), batch_size, desc='Loading wall_posts...'):
        users = current_ids[i:i+batch_size]
        try:
            code = create_execute_code(wall_params, id_list=users, method='wall.get')
            result = api.execute(code=code, timeout=60)

            if all([r == False for r in result]):
                print('empty query...')
                empty_counter += 1
            else:   
                filtered = [filter_wall_query(q) for q in result]
                db_query = [{'uid': idx, 'posts': p} for (idx, p) in zip(users, filtered)]

                db.wall_posts.insert_many(db_query)

                empty_counter = 0

            if empty_counter >= 3:
                break
                    
        except Exception as e:
            print(e)

        time.sleep(0.4)


def user_exists(uid):
    return db.users.find({'uid': uid}).count() > 0


# this function tried to load user from database
# if there is no user with given id in database it loads it
def get_user_text(uid):
    if not user_exists(uid):
        create_load_user(uid)


def get_user_params():
    params = dict()

    params['fields'] = 'photo_max_orig,personal,interests,activities,music,movies,tv,books, games, about, quotes,site,status'

    return params


def load_user_info(api):
    client = MongoClient()
    db = client.ir_project

    user_params = get_user_params()
    total_users = db.users.count()

    all_ids = set([u['uid'] for u in db.users.find()])
    # all_ids = set([u['uid'] for u in db.users.find({'$and': [{'gender': 1}, {'age': {'$lte': 30}}]})])
    downloaded_ids = set([u['uid'] for u in db.user_info.find()])

    current_ids = list(all_ids - downloaded_ids)
    batch_size = 1000

    empty_counter = 0

    for i in tqdm.trange(0, len(current_ids), batch_size, desc='Loading users info...'):
        users = current_ids[i:i+batch_size]
        try:
            result = api.users.get(**user_params, user_ids=users)
            
            if all([r == False for r in result]):
                print('empty result...')
                empty_counter += 1
            else:
                db.user_info.insert_many(result)
                empty_counter = 0

            if empty_counter >= 3:
                break

        except Exception as e:
            print(e)

        time.sleep(0.5)



def main(args):
    #txt = load_user_text(1)
    #print(txt)
    #exit(1)
    # auth_params = get_auth_params()
    sessions = [vk.AuthSession(**get_auth_params()), 
                vk.Session(access_token='384caffdac72438ecf840f594ce7c59a0a4976332a5e309c50f55d1a8fe46de70529902f0bec9859e6781'), #vlad
                vk.Session(access_token='1361403386a3b0b48437556596e8500446c16701141f5fe5e1727a1a9025bc36246092b7c500d315711d6'), #ivan
                ]


    # api = vk.API(sessions[0])
    # client = MongoClient()
    # db = client.ir_project

    # subscript_params = get_groups_params()

    # code = create_execute_code(subscript_params, id_list=[1,2], method='groups.get', user_field='user_id')
    # result = api.execute(code=code, timeout=60)

    # print(len(result))




    if args.users:
        load_users(api)

    if args.wall_posts:
        for s in sessions:
            try:
                print('changing session...')
                api_w = vk.API(s, timeout=60)
                load_wall_posts(api_w)
            except:
                continue

    if args.subscriptions:
        for s in sessions:
            try:
                print('changing session...')
                api = vk.API(s, timeout=60)
                load_subsciptions(api)
            except:
                continue

    if args.groups:
        for s in sessions:
            try:
                print('changing session...')
                api_w = vk.API(s, timeout=60)
                load_groups(api_w)
            except:
                continue

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
    parser.add_argument('--filter_wall', action='store_true', default=False,
                        help='Shall I filter response for the wall query?')
    parser.add_argument('--subscriptions', action='store_true', default=False,
                    help='Shall I load subscriptions?')
    parser.add_argument('--groups', action='store_true', default=False,
                help='Shall I load groups?')

    args = parser.parse_args()

    main(args)
