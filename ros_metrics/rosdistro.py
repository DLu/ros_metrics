import collections
import io
import git
import pathlib
import re
from tqdm import tqdm
import yaml

from .constants import distros, ros1_distros
from .metric_db import MetricDB
from .people import get_canonical_email
from .reports import get_datetime_from_dict, ONE_WEEK
from .util import version_compare


GIT_URL = 'https://github.com/ros/rosdistro.git'
REPO_PATH = pathlib.Path('cache/rosdistro')

# Patterns
ROSDEP_PATTERN = re.compile(r'rosdep/(.*)\.yaml')
DISTRO_MISC = re.compile(r'(.*)/(.*)-build.yaml')
LEGACY_X = re.compile(r'(.*)/(doc|source).yaml')
LEGACY_X2 = re.compile(r'releases/([^\-]*)\-(.*).yaml')


def get_rosdistro_repo(update=True):
    if not REPO_PATH.exists():
        repo = git.Repo.clone_from(GIT_URL, REPO_PATH)
    else:
        repo = git.Repo(REPO_PATH)
        if update:
            repo.remotes.origin.pull()
    return repo


def blob_contents(blob):
    if blob is None:
        return ''
    s = ''
    with io.BytesIO(blob.data_stream.read()) as f:
        s += f.read().decode('utf-8')
    return s


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
    a_dict = yaml.load(blob_contents(diff.a_blob))
    b_dict = yaml.load(blob_contents(diff.b_blob))
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
            yield None
            return
        diffs = 0
        for left, right, path in yaml_diff(diff):
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
            else:
                yield None
        if diffs == 0:
            # nonfunctional change - reordering keys, whitespace, etc
            yield 'update', 'release', folder
    else:
        if path == 'fuerte.yaml':
            yield 'update', 'release', 'fuerte'
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
    diff = commit.diff(parent)

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


def count_repos(db, commit_id, commit):
    try:
        db.execute('DELETE FROM repo_count WHERE commit_id={}'.format(commit_id))

        name_map = collections.defaultdict(set)
        for folder in commit.tree.trees:
            if folder.name in distros:
                distro = folder.name
                for filename in ['distribution.yaml', 'release.yaml', 'doc.yaml', 'source.yaml']:
                    try:
                        path = folder[filename]
                    except KeyError:
                        continue
                    distro_dict = yaml.load(blob_contents(path))
                    if not distro_dict.get('repositories'):
                        continue
                    these_names = set(distro_dict['repositories'].keys())
                    name_map[distro].update(these_names)
            elif folder.name == 'releases':
                for sub_blob in folder.blobs:
                    p = pathlib.Path(sub_blob.name)
                    stem = p.stem.replace('-devel', '')
                    if stem in distros:
                        distro = stem
                        distro_dict = yaml.load(blob_contents(sub_blob))
                        if 'repositories' in distro_dict:
                            names = set(distro_dict['repositories'].keys())
                            name_map[distro].update(names)
                        elif 'gbp-repos' in distro_dict:
                            gbp = distro_dict['gbp-repos']
                            if type(gbp) == list:
                                names = set([x['name'] for x in gbp])
                            else:
                                print(gbp)
                                exit(0)
                            name_map[distro].update(names)
                        else:
                            print(distro_dict)
                            exit(0)
            elif folder.name == 'doc':
                for distro_folder in folder.trees:
                    if distro_folder.name not in distros:
                        continue
                    distro = distro_folder.name
                    for filename in distro_folder.blobs:
                        p = pathlib.Path(filename.name)
                        if p.suffix == '.rosinstall':
                            name_map[distro].add(p.stem)

        all_names = set()
        for name, name_set in name_map.items():
            all_names.update(name_set)
            db.insert('repo_count', {'commit_id': commit_id, 'distro': name, 'count': len(name_set)})
        if all_names and set(name_map.keys()).intersection(set(ros1_distros)):
            db.insert('repo_count', {'commit_id': commit_id, 'distro': 'all', 'count': len(all_names)})

    except Exception as e:
        print(e)


def update_rosdistro():
    # Clone or update the repo in the cache
    repo = get_rosdistro_repo()

    commits = list(reversed(list(repo.iter_commits())))

    db = MetricDB('rosdistro')
    matched = 0
    new_matches = 0
    n = 0
    try:
        main_path = set()
        already_classified = set(db.lookup_all('commit_id', 'changes'))
        already_counted = set(db.lookup_all('commit_id', 'repo_count'))

        for commit_id, commit in enumerate(tqdm(commits)):
            main_path.add(commit.hexsha)

            n += 1

            # Check if already classified
            if commit_id not in already_classified:
                commit_dict, classifications = classify_commit(repo, main_path, commit, commit_id)
                db.update('commits', commit_dict)
                if classifications:
                    new_matches += 1
                    for classification in classifications:
                        db.insert('changes', classification)
            else:
                matched += 1

            if commit_id not in already_counted and commit_id % 100 == 0:
                count_repos(db, commit_id, commit)

    except KeyboardInterrupt:
        pass
    finally:
        n = float(n)
        print(matched, matched / n)
        print(new_matches, new_matches / n)
        db.close()


def commit_query(db, fields, clause=''):
    for commit in db.query('SELECT date, {} FROM commits INNER JOIN changes'.format(fields) +
                           ' ON commits.id = changes.commit_id {} ORDER BY date'.format(clause)):
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
    # https://www.zingchart.com/docs/chart-types/treemap


def get_repo_report(db, resolution=ONE_WEEK):
    series = collections.defaultdict(list)

    for commit in db.query('SELECT date, commit_id, distro, count FROM commits INNER JOIN repo_count'
                           ' ON commits.id = repo_count.commit_id ORDER BY date'):
        dt = get_datetime_from_dict(commit, 'date')
        key = commit['distro']
        if not series[key] or (series[key][-1][-1] != commit['count'] and dt - series[key][-1][0] > resolution):
            series[key].append((dt, commit['count']))
    return series
