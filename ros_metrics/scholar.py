from scholarly import scholarly, ProxyGenerator
from scholarly.data_types import Publication, PublicationSource
from tqdm import tqdm

from .metric_db import MetricDB
from .reports import round_time
from .util import epoch_to_datetime, now_epoch, year_month_to_datetime, get_keys

titles = ['ROS: an open-source Robot Operating System',
          'Robot Operating System 2: Design, architecture, and uses in the wild',
          'ros_control: A generic and simple control framework for ROS',
          'Rosbridge: Ros for non-ros users',
          'A generalized extended kalman filter implementation for the robot operating system',
          'An evaluation of 2D SLAM techniques available in robot operating system',
          'Model predictive control for trajectory tracking of unmanned aerial vehicles using robot operating system',
          'Layered costmaps for context-sensitive navigation',
          'Robot web tools: Efficient messaging for cloud robotics',
          'The open motion planning library']

UPDATE_FREQUENCY = 60 * 60 * 24 * 7
PROXY = None


def setup_proxy():
    global PROXY
    if PROXY is not None:
        return
    keys = get_keys().get('scholarly', {})
    if not keys:
        return

    for proxy_name, key in keys.items():
        pg = ProxyGenerator()
        proxy_cls = getattr(pg, proxy_name)
        print(f'Setting up {proxy_name} proxy...', end='')
        success = proxy_cls(key)
        if success:
            print('Success!!')
            scholarly.use_proxy(pg)
            PROXY = pg
        else:
            print('Failure!!')


def get_citation_id(title):
    setup_proxy()
    print(f'Fetching pub "{title}"')
    pub = scholarly.search_single_pub(title)
    for aid in pub['author_id']:
        if not aid:
            continue
        print(f'Fetching author {aid}')
        author = scholarly.search_author_id(aid)
        print('Filling pubs')
        scholarly.fill(author, sections=['publications'])

        for new_pub in author['publications']:
            new_title = new_pub['bib'].get('title')
            pid = new_pub.get('author_pub_id')
            if new_title == title and pid:
                print(f'Found paper {title} with ID {pid}')
                return pid
    print(f'Could not find paper {title}')


def get_citations_by_year(citation_id, fixed=True):
    pub = Publication(
        author_pub_id=citation_id,
        source=PublicationSource.AUTHOR_PUBLICATION_ENTRY,
        container_type='Publication',
        bib={}
    )
    scholarly.fill(pub)
    return pub['cites_per_year']


def update_scholar():
    db = MetricDB('scholar')

    now = now_epoch()

    try:
        citation_ids = {}
        for title in titles:
            citation_id = db.lookup('citation_id', 'papers', f'WHERE title="{title}"')
            if citation_id:
                citation_ids[citation_id] = title
                continue

            citation_id = get_citation_id(title)
            if citation_id:
                db.insert('papers', {'title': title, 'citation_id': citation_id})
                citation_ids[citation_id] = title

        bar = tqdm(citation_ids)

        for citation_id in bar:
            last_updated_at = db.lookup('last_updated_at', 'updates', f'WHERE citation_id="{citation_id}"')
            if last_updated_at is not None and now - last_updated_at < UPDATE_FREQUENCY:
                continue
            bar.set_description(f'Getting citations for {citation_ids[citation_id][:20]}...')

            for year, cites in get_citations_by_year(citation_id).items():
                entry = {'citation_id': citation_id,
                         'year': year,
                         'citations': cites}
                db.update('citations', entry, ['citation_id', 'year'])

            db.update('updates', {'last_updated_at': now, 'citation_id': citation_id}, 'citation_id')
    finally:
        db.close()


def get_ordered_papers(db):
    data = []
    for citation_id, title in db.dict_lookup('citation_id', 'title', 'papers').items():
        total = db.lookup('SUM(citations)', 'citations', f'WHERE citation_id="{citation_id}"')
        data.append((total, citation_id, title))
    return sorted(data, reverse=True)


def get_report(db):
    report = {}
    for total, citation_id, title in get_ordered_papers(db):
        series = []
        last_update = round_time(epoch_to_datetime(
            db.lookup('last_updated_at', 'updates', f'WHERE citation_id="{citation_id}"')))
        running = 0
        for row in db.query(f'SELECT year, citations FROM citations '
                            f'WHERE citation_id="{citation_id}" ORDER BY year'):
            year = row['year']
            if year == last_update.year:
                dt = last_update
            else:
                dt = year_month_to_datetime(year, 12, False)
            running += row['citations']
            series.append((dt, running))
        report[title] = series

    return report


def generate_caption():
    db = MetricDB('scholar')

    s = '<table>'
    for total, citation_id, title in get_ordered_papers(db):
        link = 'https://scholar.google.com/citations?view_op=view_citation&citation_for_view=' + citation_id
        s += f'\n<tr><td><a href="{link}">{title}</a><td>{total}'
    s += '</table>'
    return s
