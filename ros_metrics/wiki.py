import collections
import pathlib
import re
import subprocess
import time
from xml.dom.minidom import parseString

import requests

from tqdm import tqdm

from .metric_db import MetricDB
from .util import BeautifulParser, now_epoch, string_to_epoch

WIKI_URL = 'https://wiki.ros.org/'
MIRROR_PATH = pathlib.Path('cache/wiki')
PAREN_PATTERN = re.compile(r'\(([^\)]*)\)')
DOCBOOK_TEMPLATE = 'http://wiki.ros.org/action/show/{}?action=show&mimetype=text%2Fdocbook'
TWO_WEEKS_IN_SECONDS = 1209600
TAG_REMAP = {
    'revnumber': 'number',
    'authorinitials': 'user',
    'revremark': 'comment'
}

LANGUAGE_CODES = {
    'cn': 'Chinese',
    'de': 'German',
    'es': 'Spanish',
    'fr': 'French',
    'it': 'Italian',
    'ja': 'Japanese',
    'ko': 'Korean',
    'pt': 'Portuguese',
    'pt_BR': 'Brazillian Portuguese',
    'ru': 'Russian',
    'th': 'Thai',
    'tr': 'Turkish',
    'ua': 'Ukrainian',
    'vn': 'Vietnamese',
}


def sync_mirror():
    timestamp_path = MIRROR_PATH / 'wikidump.timestamp'
    if timestamp_path.exists():
        ts = open(timestamp_path).read()
        t = string_to_epoch(ts)
        now = now_epoch()
        if now - t < TWO_WEEKS_IN_SECONDS:
            return
    subprocess.call(['rsync', '-azvq', 'rsync.osuosl.org::ros_wiki_mirror', str(MIRROR_PATH),
                     '--bwlimit=200', '--delete', '--exclude', 'attachments'])


def translate_path_to_wiki(path):
    base = path.stem
    pieces = re.split(PAREN_PATTERN, base)
    for pi in range(1, len(pieces), 2):
        pieces[pi] = bytes.fromhex(pieces[pi]).decode('utf-8')
    return ''.join(pieces)


def update_pages(db):
    existing = db.dict_lookup('title', 'id', 'pages')
    for filepath in sorted(MIRROR_PATH.glob('*.html')):
        title = translate_path_to_wiki(filepath)
        if title in existing:
            continue
        db.insert('pages', {'id': len(existing), 'title': title})
        existing[title] = len(existing)


def parse_doc_book(page_name):
    url = DOCBOOK_TEMPLATE.format(page_name)
    r = requests.get(url)
    if r.status_code == 503:
        raise RuntimeError('Too Many Requests')

    raw_xml = r.content.decode()
    try:
        tree = parseString(raw_xml)
    except Exception:
        return

    for revision in tree.getElementsByTagName('revision'):
        rev_dict = {}
        for child in revision.childNodes:
            key = TAG_REMAP.get(child.tagName, child.tagName)
            rev_dict[key] = child.firstChild.data
        yield rev_dict


def get_recently_updated_pages():
    pages = set()
    r = requests.get('http://wiki.ros.org/RecentChanges?max_days=60&action=content')
    soup = BeautifulParser(r.content.decode())
    for row in soup.find_all_by_class('td', 'rcpagelink'):
        pages.add(str(row.text))
    return pages


def update_recent_edits(db):
    existing = db.dict_lookup('title', 'id', 'pages')
    for title in get_recently_updated_pages():
        if title in existing:
            # Remove last_commit tag to force recrawl
            db.update('pages', {'id': existing[title], 'last_commit': None})
        else:
            db.insert('pages', {'id': len(existing), 'title': title})
            existing[title] = len(existing)


def update_revisions(db):
    queue = list(db.query('SELECT * FROM pages WHERE last_commit IS NULL'))
    if not queue:
        return

    bar = tqdm(queue)
    for page in bar:
        bar.set_description(page['title'])
        for revision in parse_doc_book(page['title']):
            revision['number'] = int(revision['number'])
            revision['date'] = string_to_epoch(revision['date'])
            revision['page_id'] = page['id']
            db.update('revisions', revision, ['page_id', 'number'])

            if revision['number'] == 1:
                page['first_commit'] = revision['date']
            if not page.get('last_commit') or page['last_commit'] < revision['date']:
                page['last_commit'] = revision['date']

        if not page.get('last_commit'):
            page['last_commit'] = -1
        db.update('pages', page)
        time.sleep(5)


def update_wiki():
    sync_mirror()

    db = MetricDB('wiki')
    try:
        update_pages(db)
        update_recent_edits(db)
        update_revisions(db)
    finally:
        db.close()


def interesting_wiki_report(db=None):
    if db is None:
        db = MetricDB('wiki')

    existing = db.dict_lookup('id', 'title', 'pages')
    series = {'Most Edits': []}

    for row in db.query('SELECT page_id, MAX(number) FROM revisions GROUP BY page_id ORDER BY -number LIMIT 15'):
        title = existing[row['page_id']]
        url = WIKI_URL + title
        series['Most Edits'].append(((title, url), row['MAX(number)']))

    namespaces = collections.Counter()
    languages = collections.Counter()
    tutorials = collections.Counter()

    for title in existing.values():
        bits = title.split('/')
        if bits[0] in LANGUAGE_CODES:
            languages[bits[0]] += 1
        elif len(bits) > 1:
            namespaces[bits[0]] += 1

        if 'Tutorials' in bits:
            i = bits.index('Tutorials')
            key = '/'.join(bits[:i])
            tutorials[key] += 1

    series['Largest Namespaces'] = []

    for ns, count in namespaces.most_common(15):
        series['Largest Namespaces'].append(((ns, WIKI_URL + ns), count))

    series['Pages Per Language'] = []

    for language, count in languages.most_common():
        lang = LANGUAGE_CODES[language]
        series['Pages Per Language'].append(((f'{lang} ({language})', WIKI_URL + language), count))

    series['Most Tutorials'] = []
    for ns, count in tutorials.most_common(15):
        series['Most Tutorials'].append(((ns, WIKI_URL + ns), count))

    return [], series
