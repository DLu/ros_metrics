import requests
import time
from tqdm import tqdm

from .metric_db import MetricDB
from .util import now_epoch, key_subset, get_keys

config = None


def fetch_page(path, params=None, debug=False):
    global config
    if params is None:
        params = {}

    if config is None:
        config = get_keys()['discourse']

    params['api_key'] = config['key']
    params['api_username'] = config['user']
    url = config['host'] + path
    response = requests.get(url, allow_redirects=False, params=params)

    response_dict = response.json()

    if response.ok:
        return response_dict

    if response_dict.get('error_type') == 'rate_limit':
        s = max(5, response_dict['extras']['wait_seconds'] + 1)
        if debug:
            print('Waiting %d seconds' % s)
        time.sleep(s)
        # Recurse
        return fetch_page(path, params)
    else:
        raise Exception('Invalid response from {}: {}'.format(url, response.text))


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

        total = response.get('total_rows_directory_items')
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
            url = response['load_more_directory_items']
        else:
            url = None
    if bar:
        bar.close()


def fetch_user_data(db):
    results = db.query('SELECT username, id FROM users WHERE created_at IS NULL ORDER BY id')
    if not results:
        return
    for user_dict in tqdm(results, desc='Discourse user data'):
        data = fetch_page('/users/%s.json' % user_dict['username'])
        entry = key_subset(data['user'], ['id', 'admin', 'created_at', 'last_posted_at', 'last_seen_at',
                                          'moderator', 'trust_level', 'time_read'])
        entry['last_updated'] = now_epoch()
        db.update('users', entry)


def process_post(db, post, process_topic=True):
    post_d = key_subset(post, db.db_structure['tables']['posts'])
    db.update('posts', post_d)

    if not process_topic:
        return

    topic_info = {}
    for key, new_key in [('topic_id', 'id'), ('topic_title', 'name'),
                         ('category_id', 'category_id'), ('topic_slug', 'slug')]:
        if key in post and post[key]:
            topic_info[new_key] = post[key]
    db.update('topics', topic_info)


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

    while running:
        params = {}
        if min_id is not None:
            params['before'] = min_id
        posts = fetch_page('/posts.json', params).get('latest_posts', [])
        if len(posts) == 0:
            break

        new_posts = 0
        min_id = None
        for post in posts:
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

        print('Discourse - Fetched {} new posts'.format(new_posts))


def update_discourse(full=False):
    db = MetricDB('discourse')

    try:
        if full:
            full_refresh(db)
        else:
            fetch_user_data(db)
            fetch_recent_posts(db)
    finally:
        db.close()
