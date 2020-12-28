import collections
import gzip
import mailbox
import pathlib
import re

import requests

from tqdm import tqdm

from .metric_db import MetricDB
from .util import BeautifulParser, string_to_epoch

FROM_PATTERN = re.compile(r'^(.+) at ([^\(]+) \((.+)\)$')
FILE_PATTERN = re.compile(r'(\d{4}\-.*).txt.gz')
BASE_URL = 'http://lists.ros.org/pipermail/ros-users/'
FOLDER = pathlib.Path('cache/rosusers')


def download_archives():
    FOLDER.mkdir(exist_ok=True)

    root = requests.get(BASE_URL)
    soup = BeautifulParser(root.text)
    table = soup.find('table')
    for link in tqdm(table.find_all('a'), desc='ros-users downloading'):
        href = link.get('href')
        if not href or 'gz' not in href:
            continue

        output_filename = FOLDER / href
        if output_filename.exists():
            continue
        print(f'Getting {href}...')

        req = requests.get(BASE_URL + href)
        with open(output_filename, 'wb') as f:
            f.write(req.content)


def get_mailbox():
    FOLDER.mkdir(exist_ok=True)

    filtered_path = FOLDER / 'rosusers.mbox'
    if filtered_path.exists():
        return filtered_path
    full_path = FOLDER / 'full.mbox'
    if not full_path.exists():
        with open(full_path, 'w') as f:
            for archive_path in tqdm(sorted(FOLDER.glob('*.gz')), desc='ros-users unzipping'):
                with gzip.open(archive_path, 'rb') as box_zip:
                    f.write(box_zip.read().decode('utf-8', 'replace'))
                    f.write('\n\n')

    mbox = mailbox.mbox(str(full_path))
    outbox = mailbox.mbox(str(filtered_path))
    seen = set()
    for message in tqdm(mbox, desc='ros-users removing dupes'):
        mid = message['Message-Id']
        if mid in seen:
            continue
        seen.add(mid)
        outbox.add(message)
    outbox.flush()
    return filtered_path


def filter_out_discourse(msgs):
    filtered_msgs = []
    for message in tqdm(msgs, desc='ros-users discourse filter'):
        if 'ros.discourse' in str(message.get('From', '')) or 'Discourse.ros.org' in str(message.get('Subject', '')):
            continue
        filtered_msgs.append(message)
    return filtered_msgs


def group_by_reply_to(msgs):
    parents = {}
    for message in msgs:
        if message['In-Reply-To'] is None:
            parents[message['Message-Id']] = message['Message-Id']
        else:
            parents[message['Message-Id']] = message['In-Reply-To']

    for key in parents:
        new_key = key
        while parents[new_key] in parents and new_key != parents[new_key]:
            new_key = parents[new_key]
        parents[key] = new_key

    threaded = collections.defaultdict(list)

    for message in msgs:
        root = parents.get(message['Message-Id'], message['Message-Id'])
        threaded[root].append(message)

    return threaded.values()


def group_by_title(msgs):
    titles = collections.defaultdict(list)
    for message in tqdm(msgs, desc='ros-users threading'):
        title = str(message.get('Subject', ''))
        if title in ['[ros-users] (no subject)', 'No subject', '[ros-users] [no subject]']:
            title = None
        titles[title].append(message)
    threads = []
    if None in titles:
        unknowns = titles[None]
        del titles[None]
        threads += group_by_reply_to(unknowns)
    threads += titles.values()
    return threads


def get_sender(msg):
    s = str(msg.get('From', ''))
    m = FROM_PATTERN.match(s)
    if m:
        return m.group(1) + '@' + m.group(2), m.group(3)
    elif '@' in s:
        return s, ''
    else:
        return '', s


def write_threads_to_db(db, threads):
    users = {}
    c = 0

    db.reset()
    for thread_i, thread in enumerate(tqdm(threads, desc='ros-users database')):
        first = thread[0]
        title = str(first.get('Subject', ''))
        db.update('topics', {'id': thread_i, 'name': title})

        for msg in thread:
            pid = c
            c += 1
            user_key = get_sender(msg)
            if user_key in users:
                uid = users[user_key]
            else:
                uid = len(users)
                db.update('users', {'id': uid, 'email': user_key[0], 'name': user_key[1]})
                users[user_key] = uid
            raw = str(msg)

            date_string = msg['Date']
            if date_string is None:
                continue
            date = string_to_epoch(date_string)

            db.update('posts', {'id': pid, 'user_id': uid, 'topic_id': thread_i,
                                'raw': raw, 'created_at': date})


def update_ros_users(force=False):
    db = MetricDB('ros_users')

    # If the database already has some values, everything has probably already been processed, so we can skip
    if db.count('posts') > 1000 and not force:
        return

    download_archives()
    mbox_path = get_mailbox()
    mbox = mailbox.mbox(mbox_path)
    mbox = filter_out_discourse(mbox)
    threads = group_by_title(mbox)

    try:
        write_threads_to_db(db, threads)
    finally:
        db.close()
