from .rosdistro import get_rosdistro_repo, REPO_PATH
from .constants import distros
from .metric_db import MetricDB
from .util import get_github_api, now_epoch, epoch_to_datetime, datetime_to_epoch
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
GITLAB_HTTP_PATTERN = re.compile('https?://(?P<server>gitlab\.[^/]+)/(?P<org>[^/]+)/(?P<repo>.+).git')
GITLAB_SSH_PATTERN = re.compile('git@(?P<server>gitlab\.[^/]+):(?P<org>[^/]+)/(?P<repo>.+)\.git')
PATTERNS = [GITHUB_HTTP_PATTERN, GITHUB_SSH_PATTERN, BB_PATTERN, GITLAB_HTTP_PATTERN, GITLAB_SSH_PATTERN]


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
    CACHE_PATH.mkdir(exist_ok=True)

    for distro in tqdm(distros, 'updating distros'):
        distro_path = rosdistro_path / distro / 'distribution.yaml'
        if not distro_path.exists():
            continue
        distro_dict = yaml.load(open(str(distro_path)))
        repos = distro_dict['repositories']

        for name, repo in sorted(repos.items()):
            url = repo.get('source', repo.get('doc', {})).get('url')
            if not url:
                release_url = repo.get('release', {}).get('url')
                if not release_url:
                    continue
                repo_dict = match_git_host(release_url)
                if not repo_dict:
                    continue

                folder = CACHE_PATH / repo_dict['org'] / repo_dict['repo']
                if not folder.exists():
                    try:
                        git.Repo.clone_from(release_url, str(folder))
                    except Exception as e:
                        continue

                tracks_file = folder / 'tracks.yaml'
                if not tracks_file.exists():
                    continue
                tracks = yaml.load(open(tracks_file))
                if distro in tracks['tracks']:
                    url = tracks['tracks'][distro]['vcs_uri']
                else:
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
                # print(url)
                continue
            id = db.lookup('id', 'repos', 'WHERE key="{}" and url="{}"'.format(name, url))
            if id is None:
                d['key'] = name
                d['url'] = url
                d['id'] = running_count
                running_count += 1
                db.insert('repos', d)


CLONE_MESSAGES = [
    ('mercurial', 'Mercurial (hg) is required'),
    ('no_access', 'HTTP Basic: Access denied'),
    ('no_access', 'Permission denied'),
    ('no_access', 'Connection refused'),
    ('missing', 'not found'),
]


def clone(db, debug=False):
    repos = []
    to_clone = []

    CACHE_PATH.mkdir(exist_ok=True)
    for repo_dict in db.query('SELECT id, org, repo, url, status FROM repos ORDER BY id'):
        if repo_dict['status'] is not None:
            continue
        folder = CACHE_PATH / repo_dict['org'] / repo_dict['repo']
        if folder.exists():
            repos.append(git.Repo(str(folder)))
        else:
            to_clone.append((folder, repo_dict))
    if not to_clone:
        return repos
    for folder, repo_dict in tqdm(sorted(to_clone), 'cloning repos'):
        try:
            repos.append(git.Repo.clone_from(repo_dict['url'], str(folder)))
        except git.GitCommandError as e:
            for status, msg in CLONE_MESSAGES:
                if msg in e.stderr:
                    repo_dict['status'] = status
                    db.update('repos', repo_dict)
                    break

            if debug:
                print(e)
    return repos


def update(repos):
    for repo in tqdm(repos, 'updating repos'):
        repo.remotes.origin.pull()


def check_statuses(db):
    for repo_dict in tqdm(db.query('SELECT id, org, repo, url FROM repos WHERE status is null ORDER BY id'),
                          desc='checking repo status'):
        # Check for duplicates
        url = repo_dict['url']
        matches = db.lookup_all('id', 'repos', f'WHERE url="{url}" and status is null')
        if len(matches) > 1 and matches[0] == repo_dict['id']:
            for match in matches[1:]:
                repo_dict = db.query(f'SELECT * from REPOS WHERE id={match}')[0]
                repo_dict['status'] = 'dupe'
                db.update('repos', repo_dict)

        # Count packages
        folder = CACHE_PATH / repo_dict['org'] / repo_dict['repo']
        xml = list(folder.rglob('package.xml')) + list(folder.rglob('manifest.xml'))
        if len(xml) > 0:
            continue
        repo_dict['status'] = 'not_ros'
        db.update('repos', repo_dict)


def get_github_repos(db):
    repos = {}
    for repo_dict in db.query('SELECT id, org, repo, status FROM repos WHERE server="github.com"'):
        if repo_dict['status'] is not None:
            continue
        del repo_dict['status']
        repos[repo_dict['id']] = repo_dict
    return repos


def get_github_stats(db, limit=3000000):  # ~1 month
    existing_stats = db.dict_lookup('id', 'last_updated_at', 'github_stats')
    repos = get_github_repos(db)
    now = now_epoch()
    to_crawl = []
    for repo_id, repo_dict in repos.items():
        if repo_id not in existing_stats:
            to_crawl.append(repo_dict)
        elif now - existing_stats[repo_id] > limit:
            to_crawl.append(repo_dict)

    if not to_crawl:
        return

    gh = get_github_api()
    for repo_dict in tqdm(to_crawl, 'github stats'):
        try:
            repo = gh.get_repo('{org}/{repo}'.format(**repo_dict))
        except github.GithubException as e:
            if e.status == 404:
                repo_dict['status'] = 'missing'
                db.update('repos', repo_dict)
                continue
            else:
                raise e

        if repo.stargazers_count != repo.watchers_count:
            print(repo)
            print(repo.watchers_count)
            print(repo.stargazers_count)
            break

        row = {'id': repo_dict['id'],
               'forks': repo.network_count,
               'stars': repo.stargazers_count,
               'subs': repo.subscribers_count}
        row['last_updated_at'] = now
        db.update('github_stats', row)


def get_github_repo_issues(db, gh, repo_dict, last_updated_at):
    try:
        repo_str = '{org}/{repo}'.format(**repo_dict)
        repo = gh.get_repo(repo_str)
    except github.GithubException:
        return

    repo_id = repo_dict['id']
    progress = None

    if last_updated_at:
        last_updated_dt = epoch_to_datetime(last_updated_at)
    else:
        last_updated_dt = github.GithubObject.NotSet

    now = now_epoch()

    # Actually covers prs and issues
    # Note: Some PRs might get returned repeatedly if they are timestamped in the future relative to our timezone
    for issue in repo.get_issues(state='all', since=last_updated_dt):
        if progress is None and last_updated_at is None:
            progress = tqdm(total=issue.number, desc=repo_str)

        if progress:
            progress.update()

        entry = {'repo_id': repo_id, 'number': issue.number, 'username': issue.user.login, 'title': issue.title}
        entry['created_at'] = datetime_to_epoch(issue.created_at)
        if issue.pull_request:
            entry['is_pr'] = True
            pr = issue.as_pull_request()
            if pr.merged:
                entry['status'] = 'merged'
                entry['closed_at'] = datetime_to_epoch(pr.merged_at)
                if pr.merged_by:
                    entry['closer'] = pr.merged_by.login
            elif pr.state == 'closed':
                entry['status'] = issue.state
                entry['closed_at'] = datetime_to_epoch(pr.closed_at)
                if issue.closed_by:
                    entry['closer'] = issue.closed_by.login
            else:
                entry['status'] = issue.state

        else:
            entry['is_pr'] = False
            entry['status'] = issue.state
            if issue.state == 'closed':
                entry['closed_at'] = datetime_to_epoch(issue.closed_at)
                if issue.closed_by:
                    entry['closer'] = issue.closed_by.login
        db.update('github_issues', entry, ['repo_id', 'number'])

    db.update('github_issues_updates', {'id': repo_id, 'last_updated_at': now})
    if progress:
        progress.close()


def get_github_issues(db):
    repos = get_github_repos(db)
    to_crawl = []
    now = now_epoch()

    for repo_id, repo_dict in sorted(repos.items(), key=lambda d: (d[1]['org'], d[1]['repo'])):
        last_updated_at = db.lookup('last_updated_at', 'github_issues_updates', f'WHERE id={repo_id}')

        if last_updated_at:
            if now - last_updated_at < 300000:
                continue

        to_crawl.append((repo_dict, last_updated_at))

    if not to_crawl:
        return

    gh = get_github_api()
    print(gh.get_rate_limit())
    for repo_dict, last_updated_at in tqdm(to_crawl, desc='Repos: GithubIssues'):
        try:
            get_github_repo_issues(db, gh, repo_dict, last_updated_at)
        except github.RateLimitExceededException:
            print('Github limit')
            print(gh.get_rate_limit())
            return


def update_repos(local_repos=False, github_repos=True):
    db = MetricDB('repos')
    try:
        get_repo_list(db)
        if local_repos:
            repos = clone(db)
            update(repos)
            check_statuses(db)
        if github_repos:
            try:
                get_github_stats(db)
                get_github_issues(db)
            except RuntimeError as e:
                print(e)
    except KeyboardInterrupt:
        pass
    finally:
        db.close()

def github_stat_report(db):
    report = {}
    ranks = collections.defaultdict(collections.Counter)

    exclude = db.lookup_all('id', 'repos', 'WHERE status is not null')

    for repo_dict in db.query('SELECT id, forks, stars, subs from github_stats'):
        id = repo_dict['id']
        if id in exclude:
            continue
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
        repo_dict['rank_product'] = product

    return report


def get_issue_report(db):
    issue_report = {}
    for name, value in [('issues', 0), ('prs', 1)]:
        statuses = ['open', 'closed']
        if value == 1:
            statuses.append('merged')
        totals = collections.Counter()
        for status in statuses:
            clause = f'WHERE is_pr={value} AND status="{status}" GROUP BY repo_id'
            matches = db.dict_lookup('repo_id', 'count(*)', 'github_issues', clause)
            issue_report[f'{status} {name}'] = matches
            for repo_id, n in matches.items():
                totals[repo_id] += n
        issue_report[f'total {name}'] = dict(totals)
    return issue_report


def github_repos_report(db=None):
    if db is None:
        db = MetricDB('repos')
    report = github_stat_report(db)
    issue_report = get_issue_report(db)
    lines = []
    for repo_dict in sorted(db.query('SELECT * FROM repos WHERE server="github.com" and status is null'),
                            key=lambda d: report.get(d['id'], {}).get('rank_product', 0)):
        id = repo_dict['id']
        if id not in report:
            continue
        for key in ['forks', 'stars', 'subs']:
            repo_dict[key] = '{:04d} ({})'.format(report[id][key], report[id][key[:-1] + '_rank'])
        repo_dict['rank_product'] = report[id]['rank_product']
        for key in issue_report:
            repo_dict[key] = issue_report[key].get(id, '')

        # Hack to insert link
        repo_dict['repo'] = '<a href="repos/{org}_{repo}.html">{repo}</a>'.format(**repo_dict)

        lines.append(repo_dict)
    return lines


def get_open_data(db, repo_id, is_pr):
    opens = []
    closes = []

    for entry in db.query('SELECT created_at, status, closed_at FROM github_issues '
                          f'WHERE repo_id={repo_id} and is_pr={is_pr}'):
        opens.append(entry['created_at'])
        closed = entry['closed_at']
        if closed:
            closes.append(closed)
    opens.sort()
    closes.sort()

    series = []
    running = 0

    while opens or closes:
        if opens:
            if closes:
                ep = min(opens[0], closes[0])
            else:
                ep = opens[0]
        else:
            ep = closes[0]

        dt = epoch_to_datetime(ep)
        series.append((dt, running))

        while opens and opens[0] == ep:
            running += 1
            opens.pop(0)

        while closes and closes[0] == ep:
            running -= 1
            closes.pop(0)

        series.append((dt, running))

    last_updated_at = db.lookup('last_updated_at', 'github_issues_updates', f'WHERE id={repo_id}')
    series.append((epoch_to_datetime(last_updated_at), running))

    return series


def get_issues_and_prs(db, repo_id):
    return get_open_data(db, repo_id, 0), get_open_data(db, repo_id, 1)
