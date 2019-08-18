import collections
import requests
import pathlib
from tqdm import tqdm

from .link_analysis import classify_link
from .metric_db import MetricDB
from .util import get_year_month_date_range, year_month_to_datetime, find_by_class, BeautifulParser, string_to_bytes
from .util import clean_dict
from .reports import order_by_magnitude, time_buckets, normalize_timepoints, get_top_by_year

BASE_URL = 'https://awstats.osuosl.org/reports/packages.ros.org/{year}/{month:02d}/'
PAGES = {
    'root': 'awstats.packages.ros.org.html',
    'countries': 'awstats.packages.ros.org.alldomains.html',
    'urls': 'awstats.packages.ros.org.urldetail.html'
}
CACHE_FOLDER = pathlib.Path('cache/packages')


def get_filename(name, year, month):
    return '{name}_{year}_{month:02d}.html'.format(name=name, year=year, month=month)


def get_filename_url_pairs(dates):
    pairs = []
    for year, month in dates:
        url_base = BASE_URL.format(year=year, month=month)
        for name, html in PAGES.items():
            filename = CACHE_FOLDER / get_filename(name, year, month)
            url = url_base + html
            pairs.append((filename, url))
    return pairs


def scrape(pairs):
    CACHE_FOLDER.mkdir(exist_ok=True)
    not_existing = [pair for pair in pairs if not pair[0].exists()]
    if not not_existing:
        return
    for filename, url in tqdm(not_existing, desc='Packages download'):
        if filename.exists():
            continue
        req = requests.get(url)
        with open(filename, 'w') as f:
            f.write(req.text)


def interpret_data(x):
    try:
        sub = x.replace(',', '')
        return int(sub)
    except ValueError:
        try:
            return float(x)
        except ValueError:
            return x


def to_array(table, header=None, skip=0):
    skip_header = header is not None
    rows = []
    for tr in table.find_all('tr'):
        row = []
        for cell in tr.find_all(['td', 'th']):
            if len(row) == 0 and cell.find('a'):
                row.append(cell.find('a')['href'].replace('http://packages.ros.org/', ''))
            else:
                row.append(interpret_data(cell.text))
        if header is None:
            header = row
        else:
            rows.append(dict(zip(header, row[skip:])))
    if skip_header:
        return rows[1:]
    return rows


def tables_by_title(soup):
    tables = {}
    for table in soup.find_all_by_class('table', 'aws_border'):
        title_el = find_by_class(table, 'td', 'aws_title')
        if title_el:
            title = title_el.text.strip()
        else:
            title = ''
        if '(' in title:
            title = title[:title.index('(')].strip()
        tables[title] = table
    return tables


def grab_data_table(table):
    for sub_table in table.find_all('table'):
        data_table = to_array(sub_table)
        if not data_table:
            continue
        if '' in data_table[0]:
            continue
        return data_table
    return []


def parse_root(db, year, month):
    soup = BeautifulParser(open(CACHE_FOLDER / get_filename('root', year, month)))
    full_tables = tables_by_title(soup)

    # Parse Monthly History - only use the row matching the date of the report
    traffic_table = grab_data_table(full_tables['Monthly history'])
    d_s = year_month_to_datetime(year, month).strftime('%b %Y')
    for row in traffic_table:
        if row['Month'] != d_s:
            continue
        clean_dict(row, {'Month': None, 'Unique visitors': 'visitors', 'Hits': 'hits', 'Bandwidth': 'bw',
                         'Pages': None, 'Number of visits': None})
        row['bw'] = string_to_bytes(row['bw'])
        row['year'] = year
        row['month'] = month
        db.insert('traffic', row)
        break

    # Parse Operating Systems
    os_table = grab_data_table(full_tables['Operating Systems'])
    for row in os_table:
        clean_dict(row, {'Operating Systems': 'os', 'Hits': 'hits', '\xa0': None, 'Percent': None, 'Pages': None})
        row['year'] = year
        row['month'] = month
        db.insert('os', row)


def parse_countries(db, year, month):
    soup = BeautifulParser(open(CACHE_FOLDER / get_filename('countries', year, month)))
    table = soup.find_all_by_class('table', 'aws_border')[1]
    sub_table = table.find('table')
    data_table = to_array(sub_table, ['Country', 'Country Code', 'Pages', 'Hits', 'Bandwidth'], skip=1)

    # Hack because "Others" doesn't have a country code
    if data_table:
        last = data_table[-1]
        if last['Country'] == 'Others':
            last['Bandwidth'] = last['Hits']
            last['Hits'] = last['Pages']
            last['Country Code'] = 'others'

    for row in data_table:
        clean_dict(row, {'Country Code': 'cc', 'Hits': 'hits', 'Bandwidth': 'bw', 'Country': None, 'Pages': None})
        row['bw'] = string_to_bytes(row['bw'])
        row['year'] = year
        row['month'] = month
        db.insert('countries', row)


def parse_urls(db, year, month):
    soup = BeautifulParser(open(CACHE_FOLDER / get_filename('urls', year, month)))
    table = soup.find_all_by_class('table', 'aws_border')[1]
    sub_table = table.find('table')
    data_table = to_array(sub_table, ['URL', 'Viewed', 'Average size', 'Entry', 'Exit'])
    for row in data_table:
        clean_dict(row, {'URL': 'url', 'Viewed': 'hits', 'Average size': 'size', 'Entry': None, 'Exit': None})
        row['size'] = string_to_bytes(row['size'])
        row['year'] = year
        row['month'] = month
        db.insert('urls', row)


def update_packages(force=False):
    # Start at beginning of logs
    dates = get_year_month_date_range(2014, 1)

    pairs = get_filename_url_pairs(dates)

    # If all files exist, don't do any work
    if not force and all([filename.exists() for (filename, url) in pairs]):
        return

    scrape(pairs)

    db = MetricDB('packages')

    try:
        db.reset()
        for year, month in tqdm(dates, 'Packages parse'):
            parse_root(db, year, month)
            parse_countries(db, year, month)
            parse_urls(db, year, month)
    finally:
        db.close()


def package_url_filter(url):
    if url[-1] == '/':
        return None

    d = classify_link(url)
    if d and 'package' in d:
        return d['package']
    return None


def top_report(db, all_time_count=15, yearly_count=15):
    return get_top_by_year(db, 'urls', 'url', 'hits', all_time_count=all_time_count, yearly_count=yearly_count,
                           ident_tranformer=package_url_filter)


def package_ratios(db, field, values=None):
    data = collections.defaultdict(lambda: collections.defaultdict(int))

    for row in db.query('SELECT * FROM urls ORDER BY url'):
        d = classify_link(row['url'])
        if not d or field not in d:
            continue
        date = row['year'], row['month']
        v = d[field]
        data[date][v] += row['hits']
    return data


def get_package_buckets(db, table, ident_field, limit=10):
    value_dict = db.sum_counts(table, 'hits', ident_field)
    values = order_by_magnitude(value_dict, ['unknown'])
    buckets = time_buckets(db, table, values[:limit], 'year, month', ident_field, 'hits')
    return values, normalize_timepoints(buckets)
