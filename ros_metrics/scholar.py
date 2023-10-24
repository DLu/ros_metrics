from scholarly.publication import _CITATIONPUB, Publication
from scholarly._navigator import Navigator
from scholarly import scholarly

from .metric_db import MetricDB
from .reports import round_time
from .util import epoch_to_datetime, now_epoch, year_month_to_datetime

titles = ['ROS: an open-source Robot Operating System',
          'The open motion planning library']

UPDATE_FREQUENCY = 60 * 60 * 24 * 7


def get_citation_id(title):
    pub = scholarly.search_single_pub(title)
    for aid in pub.bib['author_id']:
        if not aid:
            continue
        author = scholarly.search_author_id(aid)
        author.fill(['publications'])
        for new_pub in author.publications:
            new_title = new_pub.bib.get('title')
            pid = new_pub.id_citations if hasattr(new_pub, 'id_citations') else None
            if new_title == title and pid:
                return pid


def get_citations_by_year(id_citations, fixed=False):
    if fixed:
        # This is the right way to do it (maybe) but is currently broken
        pub = Publication(Navigator(), None, 'manual')
        pub.source = 'citations'
        pub.id_citations = id_citations
        pub.fill()
        return pub.cites_per_year
    else:
        nav = Navigator()
        url = _CITATIONPUB.format(id_citations)
        soup = nav._get_soup(url)
        years = [int(y.text) for y in soup.find_all(class_='gsc_oci_g_t')]
        cites = [int(c.text) for c in soup.find_all(class_='gsc_oci_g_al')]
        return dict(zip(years, cites))


def update_scholar():
    db = MetricDB('scholar')

    now = now_epoch()
    last_updated_at = db.lookup('last_updated_at', 'updates')
    if last_updated_at is not None and now - last_updated_at < UPDATE_FREQUENCY:
        return

    try:
        for title in titles:
            citation_id = db.lookup('citation_id', 'papers', f'WHERE title="{title}"')
            if not citation_id:
                citation_id = get_citation_id(title)
                db.insert('papers', {'title': title, 'citation_id': citation_id})

            for year, cites in get_citations_by_year(citation_id).items():
                entry = {'citation_id': citation_id,
                         'year': year,
                         'citations': cites}
                db.update('citations', entry, ['citation_id', 'year'])

        db.execute('DELETE FROM updates')
        db.insert('updates', {'last_updated_at': now})
    finally:
        db.close()


def get_report(db):
    # Just the main paper for now
    citation_id = db.lookup('citation_id', 'papers', f'WHERE title="{titles[0]}"')
    series = []
    last_update = round_time(epoch_to_datetime(db.lookup('last_updated_at', 'updates')))
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
    return series
