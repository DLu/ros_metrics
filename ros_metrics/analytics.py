import apiclient.discovery
import oauth2client.service_account
import sys
from tqdm import tqdm
import collections
import datetime
from urllib.parse import urlparse

from .metric_db import MetricDB
from .reports import get_top_by_year
from .util import get_year_month_date_range, year_month_to_datetime, clean_dict, epoch_to_datetime, now_epoch, get_keys

MONTHLY_REPORTS = {'totals': {}}
YEARLY_REPORTS = {
    'url_views': {'pagePath': 'url'},
    'cc_views': {'countryIsoCode': 'cc'},
    'os_views': {'operatingSystem': 'os', 'operatingSystemVersion': 'osv'}
}
REPORT_DATA = dict()
REPORT_DATA.update(MONTHLY_REPORTS)
REPORT_DATA.update(YEARLY_REPORTS)

DOMAINS = [
    'answers.ros.org',
    'wiki.ros.org',
    'discourse.ros.org',
    'index.ros.org'
]


def get_api_service():
    api_key = get_keys()['analytics']
    scopes = ['https://www.googleapis.com/auth/analytics.readonly']
    credentials = oauth2client.service_account.ServiceAccountCredentials.from_json_keyfile_dict(api_key, scopes=scopes)
    return apiclient.discovery.build('analytics', 'v3', credentials=credentials)


def get_profiles(service):
    accounts = service.management().accounts().list().execute()
    ids = {}
    for account_d in accounts.get('items', []):
        account = account_d.get('id')
        properties = service.management().webproperties().list(accountId=account).execute()
        for property_d in properties.get('items', []):
            property = property_d.get('id')
            profiles = service.management().profiles().list(accountId=account, webPropertyId=property).execute()
            for profile_d in profiles.get('items', []):
                name = profile_d.get('name')
                # Hack for clarity
                if name == 'All Web Site Data' and profile_d.get('websiteUrl') == 'http://discourse.ros.org':
                    name = 'discourse.ros.org'
                pid = profile_d.get('id')
                ids[name] = pid
    return ids


def query(service, profile_id, metrics, dimensions=None, start_date='7daysAgo', end_date='today'):
    if isinstance(metrics, str):
        metrics = [metrics]
    if dimensions is None:
        dimensions = []

    rows = []
    start_index = 1
    bar = None
    dimensions_s = ','.join(add_prefix(dimensions))
    metrics_s = ','.join(add_prefix(metrics))

    while True:
        results = service.data().ga().get(ids=add_prefix(str(profile_id)),
                                          start_date=start_date, end_date=end_date,
                                          dimensions=dimensions_s,
                                          metrics=metrics_s,
                                          max_results=500,
                                          start_index=start_index).execute()
        if bar is None:
            bar = tqdm(total=results['totalResults'], desc=f'{profile_id} {start_date} {dimensions_s}')
        # print(yaml.dump(results))

        for row in results.get('rows', []):
            row_dict = {}
            for header, row_value in zip(results['columnHeaders'], row):
                key = header.get('name', '').replace('ga:', '')
                if header.get('dataType') == 'INTEGER' or key in ['month', 'year']:
                    row_value = int(row_value)
                row_dict[key] = row_value
            rows.append(row_dict)
            start_index += 1
            bar.update()

        if 'nextLink' not in results:
            break
    bar.close()
    return rows


def lookup_profile(service, db, profile_name):
    profile_id = db.lookup('id', 'profiles', f'WHERE name=="{profile_name}"')
    if profile_id is not None:
        return profile_id
    else:
        saved = None
        for name, id_s in get_profiles(service).items():
            db.update('profiles', {'id': int(id_s), 'name': name})
            if name == profile_name:
                saved = int(id_s)
        if saved is None:
            print(f'Cannot find profile id for {profile_name}. Existing profiles:', file=sys.stderr)
            for p_dict in db.query('SELECT * from profiles'):
                print('\t{id:9d} {name}'.format(**p_dict), file=sys.stderr)
            return None
        return saved


def get_start_point(service, db, profile_id):
    profiles = db.query(f'SELECT * from profiles WHERE id={profile_id}')
    if len(profiles) != 1:
        return None, None
    profile = profiles[0]
    year = profile['start_year']
    month = profile['start_month']
    if year is not None:
        return year, month

    # Lookup by doing a general query
    results = query(service, profile_id, 'uniquePageviews', ['year', 'month'], '2005-01-01', 'today')
    for row in results:
        if row['uniquePageviews'] == 0:
            continue
        profile['start_year'] = row['year']
        profile['start_month'] = row['month']
        db.update('profiles', profile)
        return row['year'], row['month']


def add_prefix(datum):
    if isinstance(datum, list):
        return [add_prefix(d) for d in datum]
    elif ':' in datum:
        return datum
    else:
        return 'ga:' + datum


def get_missing_data(db, profile_id, start_year, start_month):
    now = datetime.datetime.now()
    queries = []
    for table in MONTHLY_REPORTS:
        for year, month in get_year_month_date_range(start_year, start_month):
            hits = db.lookup('SUM(sessions)', table,
                             f'WHERE profile_id={profile_id} AND year={year} AND month={month}')
            if hits is None:
                start_date = year_month_to_datetime(year, month).strftime('%Y-%m-%d')
                end_date = year_month_to_datetime(year, month, beginning=False).strftime('%Y-%m-%d')
                queries.append((start_date, end_date, profile_id, table))
    for table in YEARLY_REPORTS:
        for year in range(start_year, now.year + 1):
            hits = db.lookup('SUM(pageviews)', table,
                             f'WHERE profile_id={profile_id} AND year={year}')

            if hits is not None:
                if year == now.year:
                    # Skip if already run this month
                    last_updated_at = db.lookup('last_updated_at', 'updates',
                                                f'WHERE profile_id={profile_id} and table_name="{table}"')
                    if last_updated_at is not None:
                        last_updated_at = epoch_to_datetime(last_updated_at)
                    if last_updated_at and last_updated_at.year == now.year and last_updated_at.month == now.month:
                        continue
                else:
                    continue

            start_date = year_month_to_datetime(year, 1).strftime('%Y-%m-%d')
            end_date = year_month_to_datetime(year, 12, beginning=False).strftime('%Y-%m-%d')
            queries.append((start_date, end_date, profile_id, table))
    return queries


def get_stats(service, db, profile_id, table, start_date, end_date):
    dimensions = ['year'] + list(REPORT_DATA[table].keys())
    metrics = ['uniquePageviews']
    if table in MONTHLY_REPORTS:
        dimensions.append('month')
        metrics.append('users')
        metrics.append('sessions')
    results = query(service, profile_id, metrics, dimensions, start_date, end_date)
    year = int(start_date[0:4])

    # Flush existing data (if any)
    base_flush = f'DELETE FROM {table} WHERE profile_id={profile_id} and year={year}'
    if table in MONTHLY_REPORTS:
        base_flush += ' and month={}'.format(int(start_date[5:7]))
    db.execute(base_flush)

    # n = len(results)
    # print(f'\t{profile_id} {table} {start_date} {n}')
    remapping = dict(REPORT_DATA[table])
    remapping['uniquePageviews'] = 'pageviews'
    remapping['users'] = 'users'
    remapping['sessions'] = 'sessions'

    if not results:
        row = {'profile_id': profile_id, 'year': year, 'pageviews': 0}
        db.insert(table, row)
        return

    if table == 'url_views':
        # Special handling for url values
        unique_urls = collections.Counter()
        for row in results:
            clean_dict(row, remapping)
            url = urlparse(row['url']).path
            unique_urls[url] += row['pageviews']
        for url, pageviews in unique_urls.most_common():
            row = {}
            row['profile_id'] = profile_id
            row['year'] = year
            row['pageviews'] = pageviews
            row['url'] = url
            db.insert(table, row)
    else:
        for row in results:
            clean_dict(row, remapping)
            row['profile_id'] = profile_id
            db.insert(table, row)

    db.execute(f'DELETE FROM updates WHERE profile_id={profile_id} and table_name="{table}"')
    db.insert('updates', {'profile_id': profile_id, 'table_name': table, 'last_updated_at': now_epoch()})


def update_analytics():
    service = get_api_service()

    db = MetricDB('analytics')

    queries = []
    for profile_name in DOMAINS:
        profile_id = lookup_profile(service, db, profile_name)
        if profile_id is None:
            continue

        start_year, start_month = get_start_point(service, db, profile_id)
        if start_year is None:
            continue
        queries += get_missing_data(db, profile_id, start_year, start_month)

    if not queries:
        return

    try:
        for start_date, end_date, profile_id, table in tqdm(sorted(queries), desc='Analytics updates'):
            get_stats(service, db, profile_id, table, start_date, end_date)
    finally:
        db.close()


def get_total_series(db, metric='pageviews'):
    profiles = dict([(row['id'], row['name']) for row in db.query('SELECT * FROM profiles')])
    series = collections.defaultdict(list)
    for row in db.query('SELECT * FROM totals ORDER BY year, month'):
        dt = year_month_to_datetime(row['year'], row['month'])
        name = profiles.get(row['profile_id'])
        if row[metric] or series[name]:
            series[name].append((dt, row[metric]))
    return series


def get_country_traffic(db, key='wiki'):
    # Only plot traffic to one profile
    profile_id = None
    for row in db.query('SELECT * FROM profiles'):
        if key in row['name']:
            profile_id = row['id']
            break

    ccs = [x['cc'] for x in db.query('SELECT DISTINCT cc from cc_views') if x['cc']]

    series = collections.defaultdict(list)
    base_query = f'SELECT year, pageviews FROM cc_views WHERE profile_id={profile_id}'
    for country in ccs:
        for row in db.query(base_query + f' and cc="{country}" ORDER BY year'):
            dt = year_month_to_datetime(row['year'], 1)
            series[country].append({'x': dt.isoformat(), 'y': row['pageviews']})

    return series


def wiki_url_filter(url):
    clean = url.replace('action/show/', '')
    if len(clean) > 1:
        clean = clean[1:]
    return clean, 'https://wiki.ros.org/' + clean


def top_wiki_report(db=None):
    if db is None:
        db = MetricDB('analytics')
    profile_id = db.lookup('id', 'profiles', 'WHERE name="wiki.ros.org"')
    return get_top_by_year(db, 'url_views', 'url', 'pageviews', f'WHERE profile_id={profile_id}',
                           ident_tranformer=wiki_url_filter)
