import requests
import time
from tqdm import tqdm

from .metric_db import MetricDB
from .util import now_epoch, key_subset, get_keys

config = None
USER_CRAWL_FREQUENCY = 60 * 60 * 24 * 7
USER_DATA_FREQUENCY = 60 * 60 * 24 * 30


def fetch_page(path, params=None, debug=False):
    global config

    if config is None:
        config = get_keys()['discourse']

    headers = {
        'Accept': 'application/json; charset=utf-8',
        'Api-Key': config['key'],
        'Api-Username': config['user'],
    }

    url = config['host'] + path
    response = requests.get(url, allow_redirects=False, params=params, headers=headers)

    response_dict = response.json()

    if response.ok:
        return response_dict

    if response_dict.get('error_type') == 'rate_limit':
        s = max(5, response_dict['extras']['wait_seconds'] + 1)
        if debug:
            print(f'Waiting {s} seconds')
        time.sleep(s)
        # Recurse
        return fetch_page(path, params)
    else:
        raise Exception(f'Invalid response from {url}: {response.text}')


def fetch_user_list(db):
    # The ideal way to do this is using /admin/users/list/new.json but those permissions may not be available
    url = '/directory_items.json?period=all&order=post_count'

    total = None
    bar = None

    while url:
        # Continuation urls have a bug in them
        if '.json' not in url:
            url = url.replace('?', '.json?')

        response = fetch_page(url)

        total = response.get('total_rows_directory_items') or \
            response.get('meta', {}).get('total_rows_directory_items')

        if bar is None and total:
            bar = tqdm(total=total, desc='Discourse users')

        for item in response['directory_items']:
            bar.update()
            d = item['user']
            for key in ['avatar_template', 'title']:
                del d[key]
            del item['user']
            item.update(d)
            db.update('users', item)

        if response['directory_items']:
            url = response.get('load_more_directory_items') or \
                response.get('meta', {}).get('load_more_directory_items')
        else:
            url = None
    if bar:
        bar.close()


def fetch_user_data(db, limit=500):
    now = now_epoch()
    unseen = db.query('SELECT username, id FROM users WHERE created_at IS NULL AND username IS NOT NULL ORDER BY id')

    cutoff_date = now - USER_DATA_FREQUENCY
    old = db.query(f'SELECT username, id FROM users WHERE last_updated < {cutoff_date} ORDER BY last_updated')

    to_crawl = unseen + old
    if limit:
        to_crawl = to_crawl[:limit]

    if not to_crawl:
        return
    for user_dict in tqdm(to_crawl, desc='Discourse user data'):
        try:
            username = user_dict['username']
            data = fetch_page(f'/users/{username}.json')
        except Exception as e:
            user_dict['username'] = None
            user_dict['last_updated'] = now
            db.update('users', user_dict)
            continue
        entry = key_subset(data['user'], ['id', 'admin', 'created_at', 'last_posted_at', 'last_seen_at',
                                          'moderator', 'trust_level', 'time_read'])
        entry['last_updated'] = now
        db.update('users', entry)


def process_post(db, post, process_topic=True):
    post_d = key_subset(post, db.db_structure['tables']['posts'])
    db.update('posts', post_d)

    # Process User
    user_d = {'id': post['user_id'], 'username': post['username']}
    db.update('users', user_d)

    if not process_topic:
        return

    topic_info = {}
    for key, new_key in [('topic_id', 'id'), ('topic_title', 'name'),
                         ('category_id', 'category_id'), ('topic_slug', 'slug')]:
        if key in post and post[key]:
            topic_info[new_key] = post[key]
    db.update('topics', topic_info)


def fetch_post(db, id):
    try:
        data = fetch_page(f'/posts/{id}.json')
    except Exception:
        return

    process_post(db, data, False)


def full_refresh(db):
    table_defs = db.db_structure['tables']
    # Update categories
    for category in fetch_page('/categories.json').get('category_list', {}).get('categories', []):
        db.update('categories', key_subset(category, table_defs['categories']))

    # Update Topics
    bar = tqdm(db.query('SELECT id, slug, name from categories ORDER BY id'))
    for category in bar:
        bar.set_description('Discourse Category Update ({})'.format(category['name']))
        url = '/c/{}.json'.format(category['id'])
        while url:
            data = fetch_page(url)
            for topic in data['topic_list'].get('topics', []):
                topic_dict = key_subset(topic, ['id', 'category_id', 'slug', 'views'])
                topic_dict['name'] = topic['title']
                db.update('topics', topic_dict)
                for tag in topic.get('tags', []):
                    db.insert('topic_tags', {'t_id': topic['id'], 'tag': tag})
            url = data['topic_list'].get('more_topics_url', None)
            # Fix bug:
            if url:
                url = url.replace(category['slug'], '{}.json'.format(category['id']))

    # Update posts
    for topic in tqdm(db.query('SELECT id FROM topics'), desc='Discourse topic updates'):
        topic_result = fetch_page('/t/{}.json'.format(topic['id']))
        for post in topic_result['post_stream'].get('posts', []):
            process_post(db, post, process_topic=False)


def fetch_recent_posts(db):
    # Iterate backwards until we get to the newest post currently in the db
    max_id = db.lookup('max(id)', 'posts')
    running = True
    min_id = None
    bar = tqdm(total=100, desc='Discourse Recent Posts')
    id_span = None
    last_percent = 0
    new_posts = 0

    while running:
        params = {}
        if min_id is not None:
            params['before'] = min_id
        posts = fetch_page('/posts.json', params).get('latest_posts', [])
        if len(posts) == 0:
            break

        min_id = None
        for post in posts:
            if post['id'] >= max_id:
                if id_span is None:
                    id_span = post['id'] - max_id
                if id_span == 0:
                    percent = 100
                else:
                    percent = int(100.0 * (id_span - (post['id'] - max_id)) / id_span)
                if percent > last_percent:
                    bar.update(percent - last_percent)
                    last_percent = percent
            process_post(db, post)

            if min_id is None:
                min_id = post['id']
            else:
                min_id = min(min_id, post['id'])

            # Check for completion
            if post['id'] == max_id:
                running = False
            if running:
                new_posts += 1

    bar.close()
    print(f'Discourse - Fetched {new_posts} new posts')


def update_discourse(full=False):
    db = MetricDB('discourse')

    try:
        if full:
            full_refresh(db)
        else:
            fetch_recent_posts(db)

            now = now_epoch()
            last_updated_at = db.lookup('last_updated_at', 'user_crawl')
            if last_updated_at is None or now - last_updated_at > USER_CRAWL_FREQUENCY:
                fetch_user_list(db)
                db.execute('DELETE from user_crawl')
                db.insert('user_crawl', {'last_updated_at': now})
            fetch_user_data(db)
    finally:
        db.close()
