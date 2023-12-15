import re
import stackexchange
from .metric_db import MetricDB
from .util import get_keys, datetime_to_epoch

API = None
LINK_PREFIX = '<a href="https://answers.ros.org/'
LINK_SUFFIX = r'/(\d+)/[^"]+" rel="nofollow noreferrer">'
QUESTION_PATTERN = re.compile(LINK_PREFIX + 'question' + LINK_SUFFIX)
USER_PATTERN = re.compile(r'Originally posted by ' + LINK_PREFIX + 'users' + LINK_SUFFIX)


def get_client():
    global API
    if not API:
        api_key = get_keys()['stack_exchange']['key']
        API = stackexchange.Site('api.robotics.stackexchange.com', api_key)
        API.impose_throttling = True
    return API


def process_user(db, parent_obj, field_name):
    user_json = parent_obj.json.get(field_name, {})
    if user_json.get('user_type', 'does_not_exist') == 'does_not_exist':
        return -1

    user = {
        'id': user_json['user_id'],
        'username': user_json['display_name'],
        'reputation': user_json['reputation'],
    }
    db.update('users', user)
    return user['id']


def process_answer(db, a):
    user_id = process_user(db, a, 'owner')

    answer = {
        'id': a.id,
        'q_id': a.question_id,
        'body': a.body,
        'score': a.score,
        'created_at': datetime_to_epoch(a.creation_date),
        'last_activity_at': datetime_to_epoch(a.last_activity_date),
        'user_id': user_id,
        'accepted': a.is_accepted
    }
    db.update('answers', answer)


def fetch_questions(api, db, tags=['ros', 'ros2']):
    tag_query = ';'.join(tags)
    last_activity_at = db.lookup('max(last_activity_at)', 'questions')
    qs = api.search(tagged=tag_query, sort='activity', order='asc',
                    fromdate=last_activity_at, filter='!2oiQ5JyEsJbKLNmY9lQrYT_cOGB7clpqK4hEkOccpi')
    for q in qs:
        qid = q.question_id
        # Process Tags
        existing_tags = [x['tag'] for x in db.query(f'SELECT tag FROM tags WHERE q_id="{qid}"')]
        for value in q.tags:
            if value not in existing_tags:
                db.insert('tags', {'q_id': qid, 'tag': value})

        # Process User
        user_id = process_user(db, q, 'owner')

        # Process Answers
        for answer in q.answers:
            process_answer(db, answer)

        question = {
            'id': q.question_id,
            'title': q.title,
            'user_id': user_id,
            'body': q.body,
            'link': q.link,
            'created_at': datetime_to_epoch(q.creation_date),
            'last_activity_at': datetime_to_epoch(q.last_activity_date),
            'view_count': q.view_count,
            'score': q.score,
        }
        if hasattr(q, 'accepted_answer_id'):
            question['accepted_answer_id'] = q.accepted_answer_id

        db.update('questions', question)


def update_users(api, db, batches=10, batch_size=50):
    user_ids = sorted(db.lookup_all('id', 'users', 'WHERE created_at IS NULL'))
    if not user_ids:
        return

    for batch in range(batches):
        fetch_ids = user_ids[:batch_size]
        user_ids = user_ids[batch_size:]

        users = api.users(ids=fetch_ids, filter='!40D.p(1f741kbMJEh')
        for user in users:
            entry = {
                'id': user.id,
                'username': user.display_name,
                'reputation': int(user.reputation),
                'created_at': datetime_to_epoch(user.creation_date)
            }
            db.update('users', entry)


def match_questions_to_ros(db):
    for entry in db.query('SELECT id, body FROM questions WHERE ros_id IS NULL'):
        m = QUESTION_PATTERN.search(entry['body'])
        if m:
            db.update('questions', {'id': entry['id'], 'ros_id': int(m.group(1))})
            continue


def match_answers_to_ros(db):
    for entry in db.query('SELECT id, body FROM answers WHERE ros_id IS NULL'):
        # No current way to find actual answer id, so for now we just mark as 0
        # Some day, we could cross reference the text with the answers.db
        m = USER_PATTERN.search(entry['body'])
        if m:
            db.update('answers', {'id': entry['id'], 'ros_id': 0})


def update_stack_exchange():
    api = get_client()
    db = MetricDB('stack_exchange')

    try:
        fetch_questions(api, db)
        update_users(api, db)
        match_questions_to_ros(db)
        match_answers_to_ros(db)
    finally:
        db.close()
