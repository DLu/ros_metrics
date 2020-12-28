import calendar
import collections
import csv
import datetime
import pathlib
import re

import bs4

import dateutil.parser

import github

import yaml


def find_by_class(soup, name, class_name):
    return soup.find(name, {'class': class_name})


def find_all_by_class(soup, name, class_name):
    return soup.find_all(name, {'class': class_name})


class BeautifulParser(bs4.BeautifulSoup):
    def __init__(self, obj):
        bs4.BeautifulSoup.__init__(self, obj, 'lxml')

    def find_by_class(self, name, class_name):
        return find_by_class(self, name, class_name)

    def find_all_by_class(self, name, class_name):
        return find_all_by_class(self, name, class_name)


def datetime_to_epoch(dt):
    return calendar.timegm(dt.timetuple())


def string_to_epoch(s):
    try:
        return int(s)
    except ValueError:
        dt = dateutil.parser.parse(s)
        return datetime_to_epoch(dt)


def now_epoch():
    return datetime_to_epoch(datetime.datetime.now())


epoch_to_datetime = datetime.datetime.fromtimestamp


def get_year_month_date_range(start_year, start_month):
    """Generate a list of tuples of (year, month) from (start_year, start_month) until last month."""
    today = datetime.datetime.today()
    dates = []
    # inclusive of this year
    for year in range(start_year, today.year + 1):
        if year == start_year:
            min_m = start_month
        else:
            min_m = 1

        if year == today.year:
            max_m = today.month - 1
        else:
            max_m = 12
        for month in range(min_m, max_m + 1):
            dates.append((year, month))
    return dates


def year_month_to_datetime(year, month, beginning=True):
    if beginning:
        return datetime.datetime(year, month, 1, 0, 0, 0)
    else:
        _, max_days = calendar.monthrange(year, month)
        return datetime.datetime(year, month, max_days, 0, 0, 0)


def clean_dict(d, mappings, convert_ats=True):
    for key in list(d.keys()):
        if key not in mappings:
            continue
        if convert_ats and key.endswith('_at') and isinstance(d[key], str):
            d[key] = string_to_epoch(d[key])
        dest = mappings[key]
        if dest == key:
            continue
        elif dest is not None:
            d[dest] = d[key]
        del d[key]


def key_subset(d, keys, convert_ats=True):
    new_d = {}
    for key in keys:
        if key in d and d[key]:
            if convert_ats and key.endswith('_at') and isinstance(d[key], str):
                new_d[key] = string_to_epoch(d[key])
            else:
                new_d[key] = d[key]
    return new_d


def standardize_dict(d):
    if not isinstance(d, collections.defaultdict):
        return d
    new_d = {}
    for k, v in d.items():
        new_d[k] = standardize_dict(v)
    return new_d


SIZE_PATTERN = re.compile(r'([\d\.]+)\s+([GMK]B|Bytes)')
SIZES = {
    'Bytes': 1,
    'KB': 1E3,
    'MB': 1E6,
    'GB': 1E9
}


def string_to_bytes(s):
    if type(s) == int:
        return s
    s = s.strip()
    m = SIZE_PATTERN.match(s)
    if m:
        base = float(m.group(1))
        mult = SIZES[m.group(2)]
        return int(base * mult)
    elif s == '':
        return 0
    else:
        print(repr(s))
        exit(0)


NUM = r'(\d+)'
DOT = r'\.'
SHORT_VERSION = NUM + DOT + NUM + DOT + NUM
SHORT_VERSION_PATTERN = re.compile(SHORT_VERSION)
FULL_VERSION_PATTERN = re.compile(SHORT_VERSION + r'\-' + NUM)
VERSIONS = ['major', 'minor', 'patch', 'build']


def version_compare(before, after):
    if not isinstance(before, str) or not isinstance(after, str):
        return None
    before_m = FULL_VERSION_PATTERN.match(before)
    after_m = FULL_VERSION_PATTERN.match(after)
    if not before_m or not after_m:
        # Backup - use short version
        before_m = SHORT_VERSION_PATTERN.match(before)
        after_m = SHORT_VERSION_PATTERN.match(after)
        if not before_m or not after_m:
            return None

    for i, (v1, v2) in enumerate(zip(before_m.groups(), after_m.groups())):
        if v1 != v2:
            return VERSIONS[i]


def get_manual_stats(field):
    # Get stats from aggregated csv from past reports
    for row in csv.DictReader(open('data/aggregated.csv')):
        if row[''] == field:
            del row['']
            values = {}
            for k, v in row.items():
                if v == '':
                    continue
                values[dateutil.parser.parse(k)] = int(v)
            return values


KEYS = None


def get_keys():
    global KEYS
    if KEYS is None:
        KEYS = yaml.safe_load(open('keys.yaml'))
    return KEYS


def get_github_api():
    github_token = None
    for path in [pathlib.Path.home() / '.git-tokens', pathlib.Path('keys.yaml')]:
        if not path.exists():
            continue
        values = yaml.safe_load(open(path))
        if 'github' in values:
            github_token = values['github']
            break
    if not github_token:
        raise RuntimeError('Cannot find github token')
    return github.Github(github_token)


def get_github_rate_info(gh=None):
    if gh is None:
        gh = get_github_api()

    limit = gh.get_rate_limit().core
    reset_time = limit.reset.replace(tzinfo=datetime.timezone.utc)
    limit_local = reset_time.astimezone().strftime('%-I:%M %p')

    delta = reset_time - datetime.datetime.now(datetime.timezone.utc)
    minutes = delta.seconds // 60
    seconds = delta.seconds - minutes * 60
    return f'{limit.remaining}/{limit.limit} | Resets in {minutes}m{seconds:02d}s at {limit_local}'
