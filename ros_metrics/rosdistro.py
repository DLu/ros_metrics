import collections
import pathlib
import re

import requests

from tqdm import tqdm

import yaml

from .constants import distros, ros1_distros, ros2_distros
from .metric_db import MetricDB
from .people import get_canonical_email
from .repo_utils import CloneException, blob_contents, clone_or_update, match_git_host, resolve
from .reports import ONE_WEEK, get_datetime_from_dict
from .util import standardize_dict, version_compare

GIT_URL = 'https://github.com/ros/rosdistro.git'
REPO_PATH = pathlib.Path('cache/rosdistro')

# Patterns
ROSDEP_PATTERN = re.compile(r'rosdep/(.*)\.yaml')
DISTRO_MISC = re.compile(r'(.*)/(.*)-build.yaml')
LEGACY_X = re.compile(r'(.*)/(doc|source).yaml')
LEGACY_X2 = re.compile(r'releases/([^\-]*)\-(.*).yaml')


def get_rosdistro_repo(update=True):
    return clone_or_update(GIT_URL, REPO_PATH, update)[0]


def commit_to_rosdistro(commit):
    # Multilevel dictionary:
    # First key: ROS Distro
    # Second key: Package "name"
    # Third key: Release type (release/doc/source)
    repositories = collections.defaultdict(lambda: collections.defaultdict(dict))

    for folder in commit.tree.trees:
        if folder.name in distros:
            distro = folder.name
            for filename in ['distribution.yaml', 'release.yaml', 'doc.yaml', 'source.yaml']:
                try:
                    blob = folder[filename]
                    distro_dict = yaml.safe_load(blob_contents(blob))
                except KeyError:
                    continue
                except yaml.error.YAMLError:
                    continue

                if not distro_dict.get('repositories'):
                    continue
                if filename == 'distribution.yaml':
                    for name, d in distro_dict['repositories'].items():
                        for release_type, entry in d.items():
                            repositories[distro][name][release_type] = entry
                else:
                    path = pathlib.Path(blob.path)
                    release_type = path.stem
                    for name, entry in distro_dict['repositories'].items():
                        repositories[distro][name][release_type] = entry
        elif folder.name == 'releases':
            for sub_blob in folder.blobs:
                distro = pathlib.Path(sub_blob.name).stem.replace('-devel', '')
                if distro not in distros:
                    continue

                try:
                    distro_dict = yaml.safe_load(blob_contents(sub_blob))
                except yaml.error.YAMLError:
                    continue

                release_type = 'release' if distro_dict.get('type') == 'gbp' else 'source'
                if 'repositories' in distro_dict:
                    for name, entry in distro_dict['repositories'].items():
                        repositories[distro][name][release_type] = entry
                if 'gbp-repos' in distro_dict and isinstance(distro_dict['gbp-repos'], list):
                    for entry in distro_dict['gbp-repos']:
                        url = entry['url']
                        name = pathlib.Path(url).stem.replace('-release', '')
                        repositories[distro][name]['release'] = {'url': url}
        elif folder.name == 'doc':
            for distro_folder in folder.trees:
                if distro_folder.name not in distros:
                    continue
                distro = distro_folder.name
                for filename in distro_folder.blobs:
                    if pathlib.Path(filename.name).suffix != '.rosinstall':
                        continue
                    try:
                        entry = yaml.safe_load(blob_contents(filename))
                    except yaml.error.YAMLError:
                        continue

                    # Skip badly formatted rosinstalls
                    if not isinstance(entry, list):
                        continue

                    for d in entry:
                        if len(d) == 1:
                            # Standard case
                            src_type = list(d.keys())[0]
                            info = d[src_type]
                        else:
                            # This is fixing a couple of badly formatted yaml entries for the sake of history
                            src_type = None
                            for st in ['git', 'hg', 'svn', 'bzr']:
                                if isinstance(d, dict) and st in d and d[st] is None:
                                    src_type = st
                                    info = d
                                    break
                            else:
                                raise RuntimeError('very badly formatted rosinstall')

                        repo_d = {}
                        repo_d['type'] = src_type

                        url = info.get('url', info.get('uri'))
                        if url:
                            repo_d['url'] = url
                        version = info.get('version')
                        if version:
                            repo_d['version'] = version

                        repositories[distro][info['local-name']]['doc'] = repo_d

    return standardize_dict(repositories)


def yaml_diff_iterator(a, b, keys=None):
    if keys is None:
        keys = []

    if isinstance(a, dict) and isinstance(b, dict):
        for key in set(a.keys()).union(b.keys()):
            new_keys = list(keys)
            new_keys.append(key)
            if key in a and key in b:
                for x in yaml_diff_iterator(a[key], b[key], new_keys):
                    yield x
            elif key in a:
                yield a[key], None, new_keys
            else:
                yield None, b[key], new_keys
    elif a != b:
        yield a, b, keys


def yaml_diff(diff):
    a_dict = yaml.safe_load(blob_contents(diff.a_blob))
    b_dict = yaml.safe_load(blob_contents(diff.b_blob))
    return yaml_diff_iterator(a_dict, b_dict)


def wild_array_compare(a, b, check_length=True):
    if check_length and len(a) != len(b):
        return False
    for a0, b0 in zip(a, b):
        if a0 == '*' or b0 == '*':
            continue
        elif a0 != b0:
            return False
    return True


def array_verb(a, b):
    if a is None and b is not None:
        return 'add'
    elif b is None and a is not None:
        return 'del'
    a_set = set(a)
    b_set = set(b)
    amb = a_set - b_set
    bma = b_set - a_set
    if amb and not bma:
        return 'del'
    elif bma and not amb:
        return 'add'
    else:
        return 'update'


def classify_modification(diff):
    path = diff.b_path
    path_obj = pathlib.Path(path)

    if path_obj.parts[0] in ['readme.rst', 'CONTRIBUTING.md', 'README.md', 'scripts', 'doc', 'test', '.gitignore',
                             '.github', 'ros.asc', 'ros.key', '.travis.yml', '.yamllint', 'index.yaml',
                             'index-v4.yaml', 'README'] or path_obj.suffix == '.py':
        yield 'update', 'misc', None
        return
    m = ROSDEP_PATTERN.match(path)
    if m:
        _type = m.group(1)
        diffs = 0
        for left, right, path in yaml_diff(diff):
            if not left and right:
                verb = 'add'
            elif not right and left:
                verb = 'del'
            else:
                verb = 'update'

            if _type == 'base':
                if len(path) == 1:
                    if left is None and right:
                        keys = list(right.keys())
                    elif left and right is None:
                        keys = list(left.keys())
                    else:
                        yield None
                    for key in keys:
                        yield verb, 'dep', key
                        diffs += 1

                elif len(path) >= 2:
                    yield verb, 'dep', path[1]
                    diffs += 1
            elif _type in ['python', 'ruby', 'osx-homebrew', 'gentoo']:
                yield verb, 'dep', _type
                diffs += 1
            else:
                yield None

        if diffs == 0:
            # nonfunctional change - reordering keys, whitespace, etc
            yield 'update', 'dep', _type

        return

    m = DISTRO_MISC.match(path)
    if m and m.group(1) in distros:
        yield 'update', m.group(2), m.group(1)
        return
    m = LEGACY_X.match(path) or LEGACY_X2.match(path)
    if m and m.group(1) in distros:
        distro, type_ = m.groups()
        if type_ == 'devel':
            type_ = 'release'
        elif type_ == 'dry-doc':
            type_ = 'doc'
        elif type_ in ['dependencies', 'ci-jobs']:
            yield 'update', 'misc', distro
            return
        diffs = 0
        diff_parts = list(yaml_diff(diff))
        if len(diff_parts) == 0:
            yield 'update', type_, distro
            return
        for left, right, path in diff_parts:
            if not left and right:
                verb = 'add'
            elif not right and left:
                verb = 'del'
            else:
                verb = 'update'

            if len(path) == 0 or wild_array_compare(path, ['repositories', '*']):
                yield verb, type_, distro
                diffs += 1
            elif wild_array_compare(path, ['repositories', '*', '*']):
                yield 'update', type_, distro
                diffs += 1
            else:
                yield 'update', type_, distro
        return

    if path_obj.name in ['release.yaml', 'distribution.yaml'] or \
            path in ['releases/fuerte.yaml', 'releases/groovy.yaml', 'releases/hydro.yaml']:
        if path.startswith('releases'):
            folder = path_obj.stem
        else:
            folder = path_obj.parent.name
        diffs = 0
        for left, right, path in yaml_diff(diff):
            if not left and right:
                verb = 'add'
            elif not right and left:
                verb = 'del'
            else:
                verb = 'update'

            if wild_array_compare(path, ['repositories', '*']) or (path == ['repositories'] and verb == 'add'):
                yield verb, 'package', folder
                diffs += 1
            elif wild_array_compare(path, ['repositories', '*', 'release', 'version']):
                vc = version_compare(left, right)
                if not vc:
                    vc = 'other'
                yield 'bump', vc, folder
                diffs += 1
            elif wild_array_compare(path, ['repositories', '*', 'version']):
                # Legacy version bump
                vc = version_compare(left, right)
                if not vc:
                    vc = 'other'
                yield 'bump', vc, folder
                diffs += 1
            elif verb == 'update' and wild_array_compare(path, ['repositories', '*', 'release', 'packages']):
                yield verb, 'release_packages', folder
                diffs += 1
            elif wild_array_compare(path, ['repositories', '*', 'source', 'version']):
                yield 'update', 'src', folder
                diffs += 1
            elif wild_array_compare(path, ['repositories', '*', 'doc', '*']):
                yield 'update', 'doc', folder
                diffs += 1
            elif wild_array_compare(path, ['repositories', '*', 'release']):
                yield verb, 'release', folder
                diffs += 1
            elif wild_array_compare(path, ['repositories', '*', 'release'], False):
                yield 'update', 'release', folder
                diffs += 1
            elif wild_array_compare(path, ['repositories', '*', 'source']):
                yield verb, 'source', folder
                diffs += 1
            elif wild_array_compare(path, ['repositories', '*', 'source', '*']):
                yield verb, 'source', folder
                diffs += 1
            elif wild_array_compare(path, ['repositories', '*', '*']) and 'status' in path[2]:
                yield verb, 'status', folder
                diffs += 1
            elif wild_array_compare(path, ['repositories', '*', 'doc']):
                yield verb, 'doc', folder
                diffs += 1
            elif len(path) == 0 and verb == 'add':
                yield verb, 'rosdistro', folder
                diffs += 1
            elif wild_array_compare(path, ['release_platforms', '*']):
                yield array_verb(left, right), 'release_platforms', folder
                diffs += 1
            elif wild_array_compare(path, ['repositories', '*', 'packages', '*']):
                # Legacy package listing
                yield 'update', 'release_packages', folder
                diffs += 1
            elif path == ['gbp-repos']:
                yield 'update', 'release', folder
            elif wild_array_compare(path, ['repositories', '*', '*']) and path[2] in ['url', 'uri']:
                yield verb, 'release', folder
            elif wild_array_compare(path, ['repositories', '*', 'packages']):
                yield verb, 'release_packages', folder
            elif wild_array_compare(path, ['repositories', '*', 'tags'], False):
                yield verb, 'release', folder
            elif path == ['version']:
                yield verb, 'misc', folder
            else:
                yield None
        if diffs == 0:
            # nonfunctional change - reordering keys, whitespace, etc
            yield 'update', 'release', folder
    else:
        if path == 'fuerte.yaml':
            yield 'update', 'release', 'fuerte'
            return
        elif path in ['releases/backports.yaml', 'releases/targets.yaml', 'targets.yaml', 'backports.yaml']:
            yield 'update', 'release', None
            return

        # Unknown Case
        yield None


def classify_commit(repo, main_path, commit, commit_id):
    commit_dict = {'hash': commit.hexsha, 'date': commit.authored_date, 'id': commit_id}
    commit_dict['author'] = commit.author.name
    commit_dict['email'] = commit.author.email

    if len(commit.parents) == 0:
        # Ignore first commit
        return commit_dict, None
    elif len(commit.parents) > 1:
        unseen_parents = [x for x in commit.parents if x.hexsha not in main_path]
        if len(unseen_parents) == 0:
            # Merge commit
            merge_commit = {'commit_id': commit_id, 'change_index': 0, 'verb': 'merge'}
            return commit_dict, [merge_commit]
        elif len(unseen_parents) == 1:
            # Weird Merge
            parent = unseen_parents[0]
            if len(parent.parents) == 1 and parent.parents[0].hexsha in main_path:
                # Use this as the commit instead
                commit = parent

                # Rewrite author/email to be actual committer, not merger
                commit_dict['author'] = commit.author.name
                commit_dict['email'] = commit.author.email

            else:
                # Ignore this case
                return commit_dict, None
        else:
            # Ignore this case
            return commit_dict, None

    parent = commit.parents[0]

    all_valid = True
    seen = set()
    entries = []
    for diff in parent.diff(commit):
        try:
            for ret in classify_modification(diff):
                if ret is None:
                    all_valid = False
                elif ret not in seen:
                    seen.add(ret)
                    entry = {'commit_id': commit_id, 'change_index': len(entries)}
                    entry['verb'], entry['noun'], entry['detail'] = ret
                    entries.append(entry)
        except yaml.error.YAMLError:
            entries.append({'commit_id': commit_id, 'change_index': len(entries), 'noun': 'error'})
        except Exception as e:
            print(e)
            all_valid = False
            continue

    if all_valid:
        return commit_dict, entries
    else:
        return commit_dict, None


def count_repos(db, commit_id, repositories):
    db.execute(f'DELETE FROM repo_count WHERE commit_id={commit_id}')
    all_names = set()
    for distro in repositories:
        distro_names = set(repositories[distro].keys())
        all_names.update(distro_names)
        db.insert('repo_count', {'commit_id': commit_id, 'distro': distro, 'count': len(distro_names)})

    # Skip the counts where its JUST the ROS2 repos
    if all_names and set(repositories.keys()).intersection(set(ros1_distros)):
        db.insert('repo_count', {'commit_id': commit_id, 'distro': 'all', 'count': len(all_names)})


def get_repo_id(db, repo_dict):
    pieces = [f'{key}="{value}"' for (key, value) in repo_dict.items()]
    if not pieces:
        return
    clause = 'WHERE ' + ' and '.join(pieces)
    return db.lookup('id', 'repos', clause)


def get_repo_id_from_url(db, url):
    repo_dict = match_git_host(url)
    if repo_dict:
        return get_repo_id(db, repo_dict)


def get_source_url_from_release_repo(release_url, distro):
    # Resolve/Ping it
    try:
        release_url = resolve(release_url)
    except requests.exceptions.ConnectTimeout:
        return None

    # Clone it
    try:
        _, folder = clone_or_update(release_url, update=False)
    except CloneException as e:
        return None

    tracks_file = folder / 'tracks.yaml'
    if not tracks_file.exists():
        return None

    try:
        tracks = yaml.safe_load(open(tracks_file))
    except yaml.constructor.ConstructorError:
        tracks = yaml.load(open(tracks_file))
    if distro not in tracks['tracks']:
        return None

    return tracks['tracks'][distro]['vcs_uri']


def resolve_source_url(db, entry, distro):
    # Start by checking source and doc entries
    for release_type in ['source', 'doc']:
        url = entry.get(release_type, {}).get('url')
        if url:
            return url

    # Get Release URL
    release_url = entry['release']['url']
    query = f'SELECT src_url FROM release_url_map WHERE release_url="{release_url}" AND distro="{distro}"'
    stored_url_results = db.query(query)
    if stored_url_results:
        return stored_url_results[0]['src_url']

    src_url = get_source_url_from_release_repo(release_url, distro)
    db.insert('release_url_map', {'release_url': release_url, 'src_url': src_url, 'distro': distro})


def load_repository_info(db, distro, distro_dict):
    repos = {}

    for entry in distro_dict.values():
        url = resolve_source_url(db, entry, distro)
        if url is None:
            # Can't determine source repo
            continue
        repo_dict = match_git_host(url)
        if not repo_dict:
            # Can't parse url
            continue

        parsed_branch = repo_dict.pop('branch', None)

        repo_id = get_repo_id(db, repo_dict)
        if repo_id is None:
            repo_id = db.get_next_id('repos')
            repo_dict['id'] = repo_id
            repo_dict['url'] = url
            db.insert('repos', repo_dict)

        info = {'url': url}
        version = entry.get('release', {}).get('version')
        if version:
            info['is_release'] = True
            if '-' in version:
                version, _, _ = version.partition('-')
            info['version'] = version
        else:
            info['is_release'] = False
            info['version'] = entry.get('source', entry.get('doc', {})).get('version', parsed_branch)

        repos[repo_id] = info
    return repos


def check_tags(db, commit_id, repositories, timestamp):
    all_tag_ids = set(db.lookup_all('id', 'tags'))

    for distro, distro_dict in repositories.items():
        entry = load_repository_info(db, distro, distro_dict)

        query = f'SELECT repo_id, tag, max(date), is_release FROM tags ' \
                f'WHERE distro="{distro}" and date <= {timestamp} GROUP BY repo_id'
        previous_list = db.query(query)
        previous_dict = {}
        for row in previous_list:
            previous_dict[row['repo_id']] = row['tag'], row['is_release']

        all_ids = set(previous_dict.keys()).union(set(entry.keys()))
        for repo_id in all_ids:
            if repo_id in entry:
                version = entry[repo_id]['version']
                is_release = entry[repo_id]['is_release']
                if not is_release and version is None:
                    version = 'default'
            else:
                version = None
                is_release = None

            if (version, is_release) == previous_dict.get(repo_id):
                # Nothing has changed
                continue

            d = {'repo_id': repo_id, 'distro': distro, 'tag': version, 'is_release': is_release, 'date': timestamp}

            after = db.query(f'SELECT id, tag, min(date), is_release FROM tags '
                             f'WHERE repo_id="{repo_id}" and distro="{distro}" and date > {timestamp}')
            if after:
                a_d = after[-1]
                if a_d['id'] is not None and version == a_d['tag'] and is_release == a_d['is_release']:
                    db.update('tags', {'id': a_d['id'], 'date': timestamp})
                    continue

            tag_id = len(all_tag_ids)
            while tag_id in all_tag_ids:
                tag_id += 1
            all_tag_ids.add(tag_id)

            d['id'] = tag_id
            db.insert('tags', d)
    db.insert('tags_checked', {'commit_id': commit_id})


def update_rosdistro(should_classify_commits=True, should_count_repos=True, should_check_tags=True):
    # Clone or update the repo in the cache
    repo = get_rosdistro_repo(update=True)

    commits = list(reversed(list(repo.iter_commits())))

    db = MetricDB('rosdistro')
    matched = 0
    new_matches = 0
    n = 0
    try:
        main_path = set()
        already_classified = set(db.lookup_all('commit_id', 'changes'))
        already_counted = set(db.lookup_all('commit_id', 'repo_count'))
        already_tagged = set(db.lookup_all('commit_id', 'tags_checked'))

        for commit_id, commit in enumerate(tqdm(commits, desc='rosdistro commits')):
            main_path.add(commit.hexsha)

            n += 1

            # Check if already classified
            if commit_id not in already_classified and should_classify_commits:
                commit_dict, classifications = classify_commit(repo, main_path, commit, commit_id)
                db.update('commits', commit_dict)
                if classifications:
                    new_matches += 1
                    for classification in classifications:
                        db.insert('changes', classification)
            else:
                matched += 1

            count_this_commit = should_count_repos and commit_id not in already_counted and commit_id % 100 == 0
            tag_this_commit = should_check_tags and commit_id not in already_tagged and commit_id % 1000 == 0

            # Special Case: At a brief period, ROS2 had a separate branch for its rosdistro commits.
            # This special case makes sure that the number of repos for that period are each indexed
            if should_count_repos and commit.authored_date >= 1512524340 and commit.authored_date < 1547075230:
                present_distros = set()
                for folder in commit.tree.trees:
                    if folder.name in distros:
                        present_distros.add(folder.name)

                if present_distros and len(present_distros - set(ros2_distros)) == 0:
                    count_this_commit = commit_id not in already_counted

            if count_this_commit or tag_this_commit:
                repositories = commit_to_rosdistro(commit)

                if count_this_commit:
                    count_repos(db, commit_id, repositories)

                if tag_this_commit:
                    check_tags(db, commit_id, repositories, commit.authored_date)

    except KeyboardInterrupt:
        pass
    finally:
        if should_classify_commits:
            n = float(n)
            print(matched, matched / n)
            print(new_matches, new_matches / n)
        db.close()


def get_rosdistro_repos(db):
    ids = set(db.lookup_all('id', 'repos'))
    remaps = db.dict_lookup('id', 'new_id', 'remap_repos')
    return [remaps.get(repo_id, repo_id) for repo_id in ids]


def commit_query(db, fields, clause=''):
    for commit in db.query(f'SELECT date, {fields} FROM commits INNER JOIN changes' +
                           f' ON commits.id = changes.commit_id {clause} ORDER BY date'):
        dt = get_datetime_from_dict(commit, 'date')
        yield dt, commit


def get_classification_ratio(db, resolution=ONE_WEEK):
    series = []
    last_time = None
    known = set()
    for dt, commit in commit_query(db, 'id'):
        if last_time is None:
            last_time = dt
            series.append((dt, 0))

        known.add(commit['id'])

        dt = get_datetime_from_dict(commit, 'date')
        if dt - last_time > resolution:
            last_time = dt
            series.append((dt, len(known) / commit['id']))
    return series


def get_verbs_ratio(db):
    verbs = collections.defaultdict(collections.Counter)
    for dt, commit in commit_query(db, 'verb'):
        key = dt.year, dt.month
        verbs[key][commit['verb']] += 1
    return verbs


def get_distro_action(db):
    distro_data = collections.defaultdict(collections.Counter)
    for dt, commit in commit_query(db, 'detail'):
        detail = commit['detail']
        if detail not in distros:
            continue
        key = dt.year, dt.month
        distro_data[key][detail] += 1
    return distro_data


def get_version_changes(db):
    version_data = collections.defaultdict(collections.Counter)
    for dt, commit in commit_query(db, 'noun', 'WHERE verb="bump"'):
        noun = commit['noun']
        key = dt.year, dt.month
        version_data[key][noun] += 1
    return version_data


def get_dep_changes(db):
    data = collections.defaultdict(collections.Counter)
    for dt, commit in commit_query(db, 'detail', 'WHERE noun="dep"'):
        detail = commit['detail']
        key = dt.year, dt.month
        data[key][detail] += 1
    return data


def get_people_data(db, delta, resolution=ONE_WEEK):
    total_series = []
    active_series = []

    committers = set()
    active_committers = {}
    last_time = None

    for commit in db.query('SELECT email, date FROM commits ORDER BY date'):
        email = get_canonical_email(commit['email'])
        dt = get_datetime_from_dict(commit, 'date')
        committers.add(email)
        active_committers[email] = dt

        if last_time is None or dt - last_time > resolution:
            last_time = dt
            total_series.append((dt, len(committers)))
            active_series.append((dt, len(active_committers)))

        for k in list(active_committers.keys()):
            if delta and dt - active_committers[k] >= delta:
                del active_committers[k]

    return total_series, active_series


def get_people_ratio(db):
    counts = collections.Counter()
    for commit_d in db.query('SELECT email, date FROM commits WHERE type != "merge" ORDER BY date'):
        email = get_canonical_email(commit_d['email'])
        counts[email] += 1
    return counts


def get_repo_report(db):
    series = collections.defaultdict(list)

    for commit in db.query('SELECT date, commit_id, distro, count FROM commits INNER JOIN repo_count'
                           ' ON commits.id = repo_count.commit_id ORDER BY date'):
        dt = get_datetime_from_dict(commit, 'date')
        key = commit['distro']
        if not series[key] or (series[key][-1][-1] != commit['count']):
            series[key].append((dt, commit['count']))
    return series
