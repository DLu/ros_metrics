from .rosdistro import get_rosdistro_repo, REPO_PATH
from .constants import distros
from .metric_db import MetricDB
from .util import get_github_api, now_epoch
import pathlib
import git
import github
import yaml
import collections
import re
from tqdm import tqdm

CACHE_PATH = pathlib.Path('cache/repos')
FORBIDDEN_KEYS = ['-release', 'ros.org', 'svn', 'code.google.com']

GITHUB_HTTP_PATTERN = re.compile('https?://(?P<server>github\.com)/(?P<org>[^/]+)/(?P<repo>.+)\.git')
GITHUB_SSH_PATTERN = re.compile('git@(?P<server>github\.com):(?P<org>[^/]+)/(?P<repo>.+)\.git')
BB_PATTERN = re.compile('https://(?P<server>bitbucket\.org)/(?P<org>.*)/(?P<repo>.+)')
GITLAB_PATTERN = re.compile('https?://(?P<server>gitlab\.[^/]+)/(?P<org>[^/]+)/(?P<repo>.+).git')
PATTERNS = [GITHUB_HTTP_PATTERN, GITHUB_SSH_PATTERN, BB_PATTERN, GITLAB_PATTERN]


def match_git_host(url):
    for pattern in PATTERNS:
        m = pattern.match(url)
        if m:
            return m.groupdict()


def get_raw_distro_dict(update=False):
    if update:
        get_rosdistro_repo()
    rosdistro_path = pathlib.Path(REPO_PATH)

    all_repos = collections.defaultdict(dict)

    for distro in tqdm(distros, 'updating distros'):
        distro_path = rosdistro_path / distro / 'distribution.yaml'
        if not distro_path.exists():
            continue
        distro_dict = yaml.load(open(str(distro_path)))
        repos = distro_dict['repositories']

        for name, repo in sorted(repos.items()):
            url = repo.get('source', repo.get('doc', {})).get('url')
            if not url:
                # TODO: Resolve the release repo to a source repo
                continue
            url = url.lower()
            all_repos[name][distro] = url
    return all_repos


def two_substring_match(urls, subs, return_match=False):
    if len(urls) != 2:
        return False

    a, b = urls
    if subs in a and subs not in b:
        return a if return_match else b
    if subs in b and subs not in a:
        return b if return_match else a
    return None


def get_repo_list(db):
    all_repos = get_raw_distro_dict()
    running_count = db.count('repos')
    for name, repo_dict in all_repos.items():
        urls = list(set(repo_dict.values()))
        clean_urls = []
        if len(urls) == 1:
            for key in FORBIDDEN_KEYS:
                if key in urls[0]:
                    break
            else:
                clean_urls.append(urls[0])
        elif two_substring_match(urls, 'ros2'):
            clean_urls += urls
        else:
            for key in FORBIDDEN_KEYS:
                x = two_substring_match(urls, key)
                if x:
                    clean_urls.append(x)
                    break
            else:
                # TODO: 404, 301 resolution
                pass

        for url in clean_urls:
            d = match_git_host(url)
            if not d:
                print(url)
                continue
            id = db.lookup('id', 'repos', 'WHERE key="{}" and url="{}"'.format(name, url))
            if id is None:
                d['key'] = name
                d['url'] = url
                d['id'] = running_count
                running_count += 1
                db.insert('repos', d)


def clone(db, debug=False):
    repos = []
    to_clone = []

    CACHE_PATH.mkdir(exist_ok=True)
    for repo_dict in db.query('SELECT org, repo, url FROM repos ORDER BY id'):
        folder = CACHE_PATH / repo_dict['org'] / repo_dict['repo']
        if folder.exists():
            repos.append(git.Repo(str(folder)))
        else:
            to_clone.append((folder, repo_dict['url']))
    for folder, url in tqdm(sorted(to_clone), 'cloning repos'):
        try:
            repos.append(git.Repo.clone_from(url, str(folder)))
        except Exception as e:
            if debug:
                print(e)
    return repos


def update(repos):
    for repo in tqdm(repos, 'updating repos'):
        repo.remotes.origin.pull()


def get_github_stats(db):
    existing_stats = db.lookup_all('id', 'github_stats')
    repos = []
    for repo_dict in db.query('SELECT id, org, repo FROM repos WHERE server="github.com"'):
        if repo_dict['id'] not in existing_stats:
            repos.append(repo_dict)

    gh = get_github_api()
    now = now_epoch()
    for repo_dict in tqdm(repos, 'github stats'):
        try:
            repo = gh.get_repo('{org}/{repo}'.format(**repo_dict))
        except github.GithubException:
            continue

        row = {'id': repo_dict['id'],
               'forks': repo.network_count,
               'stars': repo.stargazers_count,
               'subs': repo.subscribers_count}
        row['last_updated_at'] = now
        db.update('github_stats', row)


def update_repos():
    db = MetricDB('repos')
    try:
        get_repo_list(db)
        repos = clone(db)
        update(repos)
        get_github_stats(db)
    except KeyboardInterrupt:
        pass
    finally:
        db.close()

def github_stat_report(db):
    report = {}
    ranks = collections.defaultdict(collections.Counter)
    for repo_dict in db.query('SELECT id, forks, stars, subs from github_stats'):
        id = repo_dict['id']
        del repo_dict['id']
        for key in repo_dict:
            ranks[key][repo_dict[key]] += 1
        report[id] = repo_dict

    for repo_dict in report.values():
        my_ranks = {}
        for key in list(repo_dict.keys()):
            rank = 1
            my_value = repo_dict[key]
            for value, ct in sorted(ranks[key].items(), reverse=True):
                if value == my_value:
                    my_ranks[key] = rank
                    break
                rank += ct

        product = 1
        for key, value in my_ranks.items():
            product *= value
            new_key = key[:-1] + '_rank'
            repo_dict[new_key] = value
        repo_dict['rank'] = product
    return report


def github_repos_report(db):
    report = github_stat_report(db)
    lines = []
    for repo_dict in sorted(db.query('SELECT id, key, org, repo FROM repos WHERE server="github.com"'),
                            key=lambda d: report.get(d['id'], {}).get('rank', 0)):
        id = repo_dict['id']
        if id not in report:
            continue
        repo_dict.update(report[id])
        lines.append(repo_dict)
    return lines
