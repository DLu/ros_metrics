import scholarly
from scholarly.scholarly import _CITATIONPUB, _get_soup, _HOST
from .reports import round_time
from .metric_db import MetricDB
from .util import now_epoch, year_month_to_datetime, epoch_to_datetime


paper_ids = {
    'ROS: an open-source Robot Operating System': 'fMDLYCUAAAAJ:u5HHmVD_uO8C'
}

UPDATE_FREQUENCY = 60 * 60 * 24 * 7


def get_citations_by_year(id_citations, fixed=False):
    if fixed:
        # This is the right way to do it (maybe) but is currently broken
        pub = scholarly.Publication(None, 'manual')
        pub.source = 'citations'
        pub.id_citations = id_citations
        pub.fill()
        return pub.cites_per_year
    else:
        url = _CITATIONPUB.format(id_citations)
        soup = _get_soup(_HOST + url)
        years = [int(y.text) for y in soup.find_all(class_='gsc_vcd_g_t')]
        cites = [int(c.text) for c in soup.find_all(class_='gsc_vcd_g_al')]
        return dict(zip(years, cites))


def update_scholar():
    db = MetricDB('scholar')

    now = now_epoch()
    last_updated_at = db.lookup('last_updated_at', 'updates')
    if last_updated_at is not None and now - last_updated_at < UPDATE_FREQUENCY:
        return

    try:
        db.reset()
        for name, citation_id in paper_ids.items():
            for year, cites in get_citations_by_year(citation_id).items():
                entry = {'citation_id': citation_id,
                         'year': year,
                         'citations': cites}
                db.insert('citations', entry)
        db.insert('updates', {'last_updated_at': now})
    finally:
        db.close()


def get_report(db):
    series = []
    last_update = round_time(epoch_to_datetime(db.lookup('last_updated_at', 'updates')))
    running = 0
    for row in db.query('SELECT year, citations FROM citations ORDER BY year'):
        year = row['year']
        if year == last_update.year:
            dt = last_update
        else:
            dt = year_month_to_datetime(year, 12, False)
        running += row['citations']
        series.append((dt, running))
    return series
