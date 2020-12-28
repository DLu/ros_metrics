import collections
import pathlib
import re

from .constants import architectures, os_list, ros2_distros
from .metric_db import MetricDB
from .util import datetime_to_epoch, epoch_to_datetime, get_github_api, now_epoch

ALPHA_BETA_PATTERN = re.compile(r'(alpha|beta)(\d+)')
DATE_STRING_PATTERN = re.compile(r'\d{8}')

# List of github organization/repos to check the binaries for
# hardcoded for now
BINARY_REPOS = [('ros2', 'ros2')]

# Categories, and the list of expected values for those categories
CATEGORIES = {
    'os': {'windows', 'macos', 'linux'},
    'rosdistro': set(ros2_distros),
    'architecture': set(architectures),
    'flavor': set(os_list + ['centos']),
    'type': {'debug', 'release'},
    'dds': {'fastrtps', 'opensplice'},
}

# Dictionary from piece of a binary name to the more common name for it
REMAPPED_CATEGORY_VALUES = {
    'centos7': 'centos',
    'x86_64': 'amd64',
    'aarch64': 'arm64',
    'osx': 'macos',
}

def update_binaries():
    # Download the raw numbers from github - no categorization
    db = MetricDB('binaries')

    now = now_epoch()
    greatest = db.lookup('max(measured_at)', 'downloads')

    if greatest and epoch_to_datetime(now).month == epoch_to_datetime(greatest).month:
        return

    try:
        gh = get_github_api()

        for org, repo in BINARY_REPOS:
            repo_dict = {'org': org, 'repo': repo}
            repo_id = db.get_entry_id('binary_repos', repo_dict)
            repo = gh.get_repo(f'{org}/{repo}')

            for release in sorted(repo.get_releases(), key=lambda r: r.created_at):
                release_dict = {'repo_id': repo_id, 'name': release.title,
                                'created_at': datetime_to_epoch(release.created_at)}
                release_id = db.get_entry_id('releases', release_dict)

                for asset in release.get_assets():
                    asset_dict = {'release_id': release_id, 'name': asset.name}
                    asset_id = db.get_entry_id('assets', asset_dict)

                    db.insert('downloads', {'asset_id': asset_id, 'measured_at': now, 'count': asset.download_count})

    except KeyboardInterrupt:
        pass
    finally:
        db.close()


def categorize_binary_name(name, merge_alphabeta=True, debug=False):
    path = pathlib.Path(name)
    base = path.stem.replace('.tar', '').lower()
    parts = base.split('-')
    if parts[0] != 'ros2':
        return

    # Running list of parts of the binary name that we have not yet classified
    parts = {REMAPPED_CATEGORY_VALUES.get(part, part) for part in parts}

    categories = {}

    # Remove some bits that are often there
    for bit in ['ros2', 'package']:
        if bit in parts:
            parts.remove(bit)

    # Check known categories
    for key, values in CATEGORIES.items():
        match = values.intersection(parts)
        if len(match) == 1:
            v = list(match)[0]
            categories[key] = v
            parts.remove(v)
        elif len(match) > 1 and key == 'dds':
            # Special case - allow multiple values
            categories[key] = list(match)
            parts -= match
        elif match and debug:
            print(f'Binary {name} matched {len(match)} values in the category {key}')

    # Try to classify the remaining pieces
    for part in list(parts):
        m = ALPHA_BETA_PATTERN.match(part)
        if m:
            # Option to merge all of the alphas and betas together
            if merge_alphabeta:
                # distro is just alpha
                categories['rosdistro'] = m.group(1)
            else:
                # distro is alpha0
                categories['rosdistro'] = part
            parts.remove(part)
        elif DATE_STRING_PATTERN.match(part):
            parts.remove(part)
        elif debug:
            print(f'Binary {name} has unknown bit "{part}"')

    return categories


def get_latest_data(db):
    # Return the latest data from the binaries tables, conveniently merged together
    fields = 'count, assets.name AS asset_name, releases.name AS release_name, created_at'
    table = 'downloads ' + \
            'INNER JOIN assets ON downloads.asset_id = assets.id ' + \
            'INNER JOIN releases ON releases.id = release_id'
    latest_field = 'max(measured_at)'
    rows = []
    for row in db.query(f'SELECT {fields}, {latest_field} FROM {table} GROUP BY asset_id'):
        del row[latest_field]
        rows.append(row)
    return rows

def get_tagged_data(db, merge_alphabeta=True):
    rows = get_latest_data(db)
    for row in rows:
        d = categorize_binary_name(row['asset_name'], merge_alphabeta)
        if not d:
            continue
        row.update(d)
    return rows

def get_downloads_by_field(tagged_data, field, field1=None):
    totals = collections.OrderedDict()
    for row in sorted(tagged_data, key=lambda d: d['created_at']):
        if field not in row:
            continue

        val = row[field]
        if val not in totals:
            if field1 is None:
                totals[val] = 0
            else:
                totals[val] = collections.OrderedDict()

        if field1 is None:
            totals[val] += row['count']
        else:
            val1 = row[field1]
            if val1 not in totals[val]:
                totals[val][val1] = 0
            totals[val][val1] += row['count']
    return totals
