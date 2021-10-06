import collections

import requests

from tqdm import tqdm

from .metric_db import MetricDB
from .reports import ONE_WEEK, get_datetime_from_dict
from .util import clean_dict, now_epoch

# Askbot API does not require a key
SERVER = 'https://answers.ros.org'


def fetch_page(name, page=None, sort=None):
    url = f'{SERVER}/api/v1/{name}/?'
    params = []
    if page:
        params.append(f'page={page}')
    if sort:
        params.append(f'sort={sort}')
    url += '&'.join(params)
    req = requests.get(url)
    content_type = req.headers.get('content-type')
    if content_type == 'application/json':
        return req.json()
    else:
        raise Exception(f'Bad Content Type "{content_type}" for {url}')


def update_user(db, item):
    clean_dict(item, {'avatar': None, 'joined_at': 'created_at', 'answers': None, 'questions': None, 'comments': None})
    item['last_crawl_at'] = now_epoch()
    db.update('users', item)


def fetch_user(db, user_id):
    try:
        contents = fetch_page(f'users/{user_id}')
        update_user(db, contents)
    except Exception:
        # Likely the user was deleted
        db.update('users', {'id': user_id, 'last_crawl_at': -1})


def fetch_user_list(db, force=False):
    # Sorting is currently broken in the askbot api
    # https://answers.ros.org/api/v1/users/?sort=oldest
    # and
    # https://answers.ros.org/api/v1/users/?sort=recent
    # should be different
    if force:
        start_page = 0
    else:
        start_page = max(db.count('users') // 10 - 1, 0)

    response = fetch_page('users', start_page, 'oldest')
    for page in tqdm(range(start_page, response['pages'])):
        for item in response['users']:
            update_user(db, item)
        response = fetch_page('users', page + 1, 'oldest')


def process_question(db, item):
    qid = item['id']

    # Process Tags
    existing_tags = [x['tag'] for x in db.query(f'SELECT tag FROM tags WHERE q_id="{qid}"')]
    for value in item['tags']:
        if value not in existing_tags:
            db.insert('tags', {'q_id': qid, 'tag': value})

    # Process Answers
    for aid in item.get('answer_ids', []):
        answer = {'q_id': qid, 'id': aid}
        if aid == item.get('accepted_answer_id'):
            answer['accepted'] = True
        fetch_answer(db, aid, answer)

    # Process Question Itself
    # Start by removing a bunch of fields
    clean_dict(item,
               {'avatar': None,
                'added_at': 'created_at',
                'tags': None,
                'answer_count': None,
                'answer_ids': None,
                'last_activity_by': None,
                'last_edited_at': None,
                'last_edited_by': None,
                'closed_by': None,
                'closed_at': None,
                'closed_reason': None})
    if 'closed' in item:
        if item['closed'] and 'accepted_answer_id' not in item:
            item['accepted_answer_id'] = -1
        del item['closed']
    item['user_id'] = item['author']['id']
    del item['author']
    item['last_crawl_at'] = now_epoch()
    db.update('questions', item)


def fetch_question(db, qid):
    page = fetch_page(f'questions/{qid}')
    process_question(db, page)


def fetch_questions(db, forward=False):
    if forward:
        page = max(db.count('questions') // 50 + 1, 1)
        sort_type = 'activity-asc'
    else:
        page = 0
        sort_type = 'activity-desc'
    bar = None
    new_qs = 0

    now = now_epoch()
    if not forward:
        last_activity = db.lookup('max(last_activity_at)', 'questions')
        time_delta = now - last_activity
        bar = tqdm(total=100, desc='answers.ros.org question crawl')
        last_percent = 0

    while page is not None:
        response = fetch_page('questions', page, sort_type)
        if bar is None and forward:
            bar = tqdm(total=response['pages'] - page, desc='answers.ros.org question crawl')
        for item in response['questions']:
            if not forward:
                q_activity = int(item['last_activity_at'])
                if now < q_activity:  # Because sometimes the remote server has different timestamps than local
                    now = q_activity
                    time_delta = now - last_activity
                if time_delta != 0:
                    percent = int(100.0 * (now - q_activity) / time_delta)
                else:
                    percent = 0
                if percent > last_percent:
                    bar.update(percent - last_percent)
                    last_percent = percent

                if last_activity >= q_activity:
                    page = None
                    break

            process_question(db, item)

            if not forward and page is not None:
                new_qs += 1
        if page is not None:
            page += 1
        if forward:
            bar.update()
        if forward and page >= response['pages']:
            break
    bar.close()


def fetch_answer(db, aid, initial_dict=None):
    if initial_dict is not None:
        answer_dict = initial_dict
    else:
        answer_dict = {}
        answer_dict['id'] = aid

    try:
        contents = fetch_page(f'answers/{aid}')
        answer_dict['votes'] = contents['score']
        answer_dict['created_at'] = int(contents['added_at'])
        answer_dict['user_id'] = contents['author']['id']
        # TODO: last_activity_at
    except Exception:
        pass

    db.update('answers', answer_dict)


def update_users(db, limit=None):
    users = set()
    users.update(set(db.lookup_all('user_id', 'questions')))
    users.update(set(db.lookup_all('user_id', 'answers')))
    missing = []
    no_crawl = []
    for user in users:
        if user is None:
            continue
        results = db.query(f'SELECT last_crawl_at FROM users WHERE id={user}')
        if not results:
            missing.append(user)
        else:
            last_crawl = results[0]['last_crawl_at']
            if last_crawl is None:
                no_crawl.append(user)

    to_crawl = sorted(missing) + sorted(no_crawl)
    if limit:
        to_crawl = to_crawl[:limit]

    if not to_crawl:
        return

    for user in tqdm(to_crawl, desc='answers.ros.org user updates'):
        fetch_user(db, user)


def manual_closing_check(db):
    for d in tqdm(db.query('SELECT id FROM questions WHERE accepted_answer_id is null ORDER BY created_at')):
        try:
            fetch_question(db, d['id'])
        except KeyboardInterrupt:
            break
        except Exception:
            db.update('questions', {'id': d['id'], 'accepted_answer_id': -1})
            continue


def update_answers():
    db = MetricDB('answers')

    try:
        fetch_questions(db)
        update_users(db)
    finally:
        db.close()


def answered_report(db, resolution=ONE_WEEK):
    answered_questions_series = []
    closed_questions_series = []
    ratios_series = []
    answered = 0
    closed = 0

    last_time = None
    total_q = 0.0
    for user_dict in db.query('SELECT * FROM questions ORDER BY created_at, id'):
        if user_dict['created_at'] is None:
            continue
        total_q += 1.0
        dt = get_datetime_from_dict(user_dict, 'created_at')

        accepted = user_dict['accepted_answer_id']
        if accepted is None:
            pass
        elif accepted < 0:
            closed += 1
        else:
            answered += 1

        if last_time is None or dt - last_time > resolution:
            last_time = dt
            answered_questions_series.append((dt, answered))
            closed_questions_series.append((dt, closed))
            ratios_series.append((dt, round(answered / total_q, 3)))
    return answered_questions_series, closed_questions_series, ratios_series


def karma_report(db):
    karma_ranges = collections.OrderedDict()
    karma_ranges['<= 10'] = 10
    karma_ranges['11 - 100'] = 100
    karma_ranges['101 - 500'] = 500
    karma_ranges['501 - 1000'] = 1000
    karma_ranges['1001 - 2000'] = 2000
    karma_ranges['2000+'] = None
    karma_buckets = collections.Counter()
    for user_row in db.query('SELECT reputation from users'):
        reputation = user_row['reputation']
        if reputation is None:
            continue
        for k, v in karma_ranges.items():
            if v is None or reputation <= v:
                karma_buckets[k] += 1
                break

    report = []
    for key in karma_ranges:
        report.append((key, karma_buckets[key]))
    return report


def get_top_users(db=None, all_time_count=15, yearly_count=15, by_votes=False):
    if db is None:
        db = MetricDB('answers')
    totals = collections.Counter()
    per_year = collections.defaultdict(collections.Counter)

    users = {}

    for answer in db.query('SELECT votes, created_at, user_id FROM answers'):
        if not answer['created_at'] or not answer['user_id']:
            continue
        dt = get_datetime_from_dict(answer, 'created_at')
        key = dt.year

        uid = answer['user_id']
        if uid not in users:
            users[uid] = db.lookup('username', 'users', f'WHERE id={uid}')

        author = users[uid], f'https://answers.ros.org/users/{uid}/{users[uid]}/'
        points = 1 + answer['votes'] if by_votes else 1

        totals[author] += points
        per_year[key][author] += points

    yearly = {}
    for year in per_year:
        yearly[year] = list(per_year[year].most_common(yearly_count))
    all_time = list(totals.most_common(all_time_count))
    return all_time, yearly


def get_top_questions(db=None, q_count=10):
    if db is None:
        db = MetricDB('answers')

    series = {}

    question_queries = [
        ('Top Scoring Questions', 'score'),
        ('Top Viewed Questions', 'view_count'),
    ]
    for title, metric in question_queries:
        values = []
        for q in db.query(f'SELECT * from questions WHERE {metric} is not NULL ORDER BY -{metric} LIMIT {q_count}'):
            values.append(((q['title'], q['url']), q[metric]))
        series[title] = values

    answers = db.unique_counts('answers', 'q_id')
    most_answers = []
    for q_id, num_answers in sorted(answers.items(), key=lambda d: d[1], reverse=True)[:q_count]:
        q = db.query(f'SELECT title, url from questions WHERE id={q_id}')[0]
        most_answers.append(((q['title'], q['url']), num_answers))
    series['Most Answered Questions'] = most_answers

    good_answers = []
    for answer in db.query(f'SELECT * FROM answers WHERE votes is not NULL ORDER BY -votes LIMIT {q_count}'):
        q_id = answer['q_id']
        a_id = answer['id']
        u_id = answer['user_id']
        url = f'{SERVER}/question/{q_id}/?answer={a_id}#post-id-{a_id}'
        title = db.lookup('title', 'questions', f'WHERE id={q_id}')
        user = db.lookup('username', 'users', f'WHERE id={u_id}')
        text = user + ' @ ' + title
        good_answers.append(((text, url), answer['votes']))
    series['Top Scoring Answers'] = good_answers

    return [], series
