import collections

import git

import github

import requests

from tqdm import tqdm

from .metric_db import MetricDB
from .repo_utils import CloneException, clone_or_update, get_cache_folder, match_git_host, resolve
from .reports import ONE_WEEK
from .rosdistro import get_repo_id, get_rosdistro_repos
from .util import datetime_to_epoch, epoch_to_datetime, get_github_api, get_github_rate_info, now_epoch


def check_urls(rosdistro_db):
    results = rosdistro_db.query('SELECT id, org, repo, url FROM repos WHERE status is null ORDER BY id')
    if not results:
        return
    for repo_dict in tqdm(results, 'checking repo urls'):
        try:
            new_url = resolve(repo_dict['url'])
            if new_url != repo_dict['url']:
                repo_dict2 = match_git_host(new_url)
                if repo_dict2 is None:
                    continue
                new_id = get_repo_id(rosdistro_db, repo_dict2)
                if new_id is None:
                    new_id = rosdistro_db.get_next_id('repos')
                    repo_dict2['url'] = new_url
                    repo_dict2['id'] = new_id
                    rosdistro_db.insert('repos', repo_dict2)
                repo_dict['status'] = 'remap'
                rosdistro_db.insert('remap_repos', {'id': repo_dict['id'], 'new_id': new_id})
            else:
                repo_dict['status'] = 'ok'
        except requests.exceptions.ConnectTimeout:
            repo_dict['status'] = 'missing'
        rosdistro_db.update('repos', repo_dict)


def clone(rosdistro_db, repos_db, rosdistro_ids, debug=False):
    repos = {}
    to_clone = []

    for repo_dict in rosdistro_db.query('SELECT * FROM repos WHERE status = "ok" ORDER BY id'):
        if repo_dict['id'] not in rosdistro_ids:
            continue
        folder = get_cache_folder(repo_dict)
        if folder.exists():
            repos[repo_dict['id']] = git.Repo(str(folder))
        else:
            to_clone.append((folder, repo_dict))

    if not to_clone:
        return repos

    ts = now_epoch()
    for folder, repo_dict in tqdm(sorted(to_clone, key=lambda pair: pair[0]), 'cloning repos'):
        repo_id = repo_dict['id']
        try:
            repo, path = clone_or_update(repo_dict['url'], folder)
            repos[repo_id] = repo
            repos_db.update('repo_updates', {'id': repo_id, 'last_updated_at': ts})
        except CloneException as e:
            repo_dict['status'] = e.message
            rosdistro_db.update('repos', repo_dict)
    return repos


def update(repos_db, repos, update_period=300000):
    ts = now_epoch()
    to_update = []
    for repo_id, repo in repos.items():
        last_updated_at = repos_db.lookup('last_updated_at', 'repo_updates', f'WHERE id={repo_id}')
        if last_updated_at and ts - last_updated_at < update_period:
            continue
        to_update.append((repo_id, repo))

    if not to_update:
        return

    for repo_id, repo in tqdm(to_update, 'updating repos'):
        try:
            repo.remotes.origin.pull()
            repos_db.update('repo_updates', {'id': repo_id, 'last_updated_at': ts})
        except git.GitCommandError as e:
            print(repo, e)


def check_statuses(rosdistro_db, rosdistro_ids):
    for repo_dict in tqdm(rosdistro_db.query('SELECT * FROM repos WHERE status = "ok" ORDER BY id'),
                          desc='checking repo status'):
        if repo_dict['id'] not in rosdistro_ids:
            continue
        # Count packages
        folder = get_cache_folder(repo_dict)
        xml = list(folder.rglob('package.xml')) + list(folder.rglob('manifest.xml'))
        if len(xml) > 0:
            continue
        repo_dict['status'] = 'not_ros'
        rosdistro_db.update('repos', repo_dict)


def get_github_repos(rosdistro_db, rosdistro_ids=None):
    if rosdistro_ids is None:
        rosdistro_ids = get_rosdistro_repos(rosdistro_db)
    repos = {}
    for repo_dict in rosdistro_db.query('SELECT id, org, repo FROM repos WHERE status="ok" and server="github.com"'):
        if repo_dict['id'] not in rosdistro_ids:
            continue
        repos[repo_dict['id']] = repo_dict
    return repos


def get_github_stats(rosdistro_db, repos_db, github_repos, limit=3000000):  # ~1 month
    existing_stats = repos_db.dict_lookup('id', 'last_updated_at', 'github_stats')
    now = now_epoch()
    to_crawl = []
    for repo_id, repo_dict in github_repos.items():
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
                rosdistro_db.update('repos', repo_dict)
            continue

        row = {'id': repo_dict['id'],
               'forks': repo.network_count,
               'stars': repo.stargazers_count,
               'subs': repo.subscribers_count}
        row['last_updated_at'] = now
        repos_db.update('github_stats', row)


def get_github_repo_issues(repos_db, gh, repo_dict, last_updated_at):
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
        repos_db.update('github_issues', entry, ['repo_id', 'number'])

    repos_db.update('github_issues_updates', {'id': repo_id, 'last_updated_at': now})
    if progress:
        progress.close()


def get_github_issues(repos_db, github_repos):
    to_crawl = []
    now = now_epoch()

    for repo_id, repo_dict in sorted(github_repos.items(), key=lambda d: (d[1]['org'], d[1]['repo'])):
        last_updated_at = repos_db.lookup('last_updated_at', 'github_issues_updates', f'WHERE id={repo_id}')

        if last_updated_at:
            if now - last_updated_at < 300000:
                continue

        to_crawl.append((repo_dict, last_updated_at))

    if not to_crawl:
        return

    gh = get_github_api()
    for repo_dict, last_updated_at in tqdm(to_crawl, desc='Repos: GithubIssues'):
        try:
            get_github_repo_issues(repos_db, gh, repo_dict, last_updated_at)
        except github.RateLimitExceededException:
            print(get_github_rate_info(gh))
            return


def update_repos(local_repos=False, check_github_repos=True):
    rosdistro_db = MetricDB('rosdistro')
    repos_db = MetricDB('repos')
    try:
        check_urls(rosdistro_db)
        rosdistro_ids = get_rosdistro_repos(rosdistro_db)
        if local_repos:
            repos = clone(rosdistro_db, repos_db, rosdistro_ids)
            update(repos_db, repos)
            check_statuses(rosdistro_db, rosdistro_ids)
        if check_github_repos:
            gh = get_github_api()
            limit = gh.get_rate_limit().core
            if limit.remaining > 0:
                try:
                    github_repos = get_github_repos(rosdistro_db, rosdistro_ids)
                    get_github_stats(rosdistro_db, repos_db, github_repos)
                    get_github_issues(repos_db, github_repos)
                except RuntimeError as e:
                    print(get_github_rate_info(gh))
                    print(e)
            else:
                print(get_github_rate_info(gh))
    except KeyboardInterrupt:
        pass
    finally:
        repos_db.close()
        rosdistro_db.close()


def github_stat_report(rosdistro_db, repos_db, github_repos):
    report = {}
    ranks = collections.defaultdict(collections.Counter)

    exclude = rosdistro_db.lookup_all('id', 'repos', 'WHERE status != "ok"')

    for repo_dict in repos_db.query('SELECT id, forks, stars, subs from github_stats'):
        repo_id = repo_dict['id']
        if repo_id in exclude:
            continue
        del repo_dict['id']
        for key in repo_dict:
            ranks[key][repo_dict[key]] += 1
        report[repo_id] = repo_dict

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


def github_repos_report(repos_db=None):
    if repos_db is None:
        repos_db = MetricDB('repos')
    rosdistro_db = MetricDB('rosdistro')
    github_repos = get_github_repos(rosdistro_db)

    report = github_stat_report(rosdistro_db, repos_db, github_repos)
    issue_report = get_issue_report(repos_db)
    lines = []
    for repo_id, repo_dict in github_repos.items():
        repo_dict.update(report[repo_id])
        for key in issue_report:
            repo_dict[key] = issue_report[key].get(repo_id, '')

        lines.append(repo_dict)
    return lines


def get_open_data(db, repo_id, is_pr, simplified=False):
    opens = []
    closes = []

    if repo_id is not None:
        clause = f' and repo_id={repo_id}'
    else:
        clause = ''

    for entry in db.query('SELECT created_at, status, closed_at FROM github_issues '
                          f'WHERE is_pr={is_pr}' + clause):
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
        if not simplified:
            series.append((dt, running))

        while opens and opens[0] == ep:
            running += 1
            opens.pop(0)

        while closes and closes[0] == ep:
            running -= 1
            closes.pop(0)

        if not simplified or not series or dt - series[-1][0] > ONE_WEEK:
            series.append((dt, running))

    if repo_id is not None:
        last_updated_at = db.lookup('last_updated_at', 'github_issues_updates', f'WHERE id={repo_id}')
        if last_updated_at:
            series.append((epoch_to_datetime(last_updated_at), running))

    return series


def get_issues_and_prs(db, repo_id, simplified=False):
    return get_open_data(db, repo_id, 0, simplified), get_open_data(db, repo_id, 1, simplified)


def get_total_repo_data(db, repo_id, is_pr, simplified=False):
    data = collections.defaultdict(list)
    counts = collections.Counter()
    name = 'prs' if is_pr else 'issues'

    if repo_id is not None:
        clause = f' and repo_id={repo_id}'
    else:
        clause = ''

    for entry in db.query('SELECT created_at FROM github_issues '
                          f'WHERE is_pr={is_pr}' + clause + ' ORDER BY created_at'):
        dt = epoch_to_datetime(entry['created_at'])
        key = f'{name} opened'
        counts[key] += 1

        if not simplified or not data[key] or dt - data[key][-1][0] > ONE_WEEK:
            data[key].append((dt, counts[key]))

    for entry in db.query('SELECT status, closed_at FROM github_issues '
                          f'WHERE status != "open" and is_pr={is_pr}' + clause + ' ORDER BY closed_at'):
        dt = epoch_to_datetime(entry['closed_at'])
        status = entry['status']
        key = f'{name} {status}'
        counts[key] += 1

        if not simplified or not data[key] or dt - data[key][-1][0] > ONE_WEEK:
            data[key].append((dt, counts[key]))

    return data


def get_total_issues_and_prs(db, repo_id=None, simplified=False):
    data = get_total_repo_data(db, repo_id, 0, simplified)
    data.update(get_total_repo_data(db, repo_id, 1, simplified))
    return data
