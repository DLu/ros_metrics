import git

from tqdm import tqdm

from .metric_db import MetricDB
from .people import get_canonical_email
from .repo_utils import find_manifests
from .reports import ONE_WEEK, get_datetime_from_dict
from .repos import clone
from .rosdistro import get_repo_name, get_rosdistro_repos

def get_commits(repo, tags, ignore_list=set()):
    """Retrieve all commmits with lineage from one of the released tags."""
    commit_map = {}
    for tag in tags:
        try:
            for commit in repo.iter_commits(tag):
                if commit.authored_date < 1040000000:  # Skip a few commits from before 2002
                    continue
                if commit.hexsha in ignore_list:
                    continue
                commit_map[commit.hexsha] = commit
        except git.GitCommandError:
            continue

    commits = list(commit_map.values())
    commits.sort(key=lambda d: d.authored_date)

    return commits

def is_valid_commit(commit):
    results = list(find_manifests(commit.tree))
    return len(results) > 0


def update_commit_list(db, repo, all_tags, repo_id):
    next_id = db.lookup('max(id)', 'commits') + 1

    validated_commits = db.dict_lookup('hash', 'id', 'commits', f'WHERE repo_id={repo_id} AND valid IS NOT NULL')
    commits = get_commits(repo, all_tags, validated_commits)
    if not commits:
        return
    unvalidated_commits = db.dict_lookup('hash', 'id', 'commits', f'WHERE repo_id={repo_id} AND valid IS NULL')

    seen_valid_package = False
    for commit in tqdm(commits, 'commits'):
        if commit.hexsha in unvalidated_commits:
            commit_id = unvalidated_commits[commit.hexsha]
            if not seen_valid_package:
                seen_valid_package = is_valid_commit(commit)
            db.update('commits', {'id': commit_id, 'valid': seen_valid_package})
            continue

        commit_dict = {'repo_id': repo_id, 'hash': commit.hexsha, 'date': commit.authored_date}
        commit_dict['author'] = commit.author.name
        commit_dict['email'] = commit.author.email
        commit_dict['id'] = next_id
        commit_dict['valid'] = is_valid_commit(commit)
        next_id += 1
        db.insert('commits', commit_dict)


def update_commits():
    rosdistro_db = MetricDB('rosdistro')
    repos_db = MetricDB('repos')
    db = MetricDB('commits')

    rosdistro_ids = get_rosdistro_repos(rosdistro_db)

    try:
        bar = tqdm(rosdistro_ids)
        for repo_id in bar:
            repos = clone(rosdistro_db, repos_db, [repo_id])
            if repo_id not in repos:
                continue
            repo = repos[repo_id]
            name = get_repo_name(rosdistro_db, repo_id)
            bar.set_description(f'examining commits {name:30s}')

            all_tags = rosdistro_db.lookup_all('tag', 'tags', f'WHERE repo_id={repo_id}')

            update_commit_list(db, repo, all_tags, repo_id)
    except KeyboardInterrupt:
        pass
    finally:
        db.close()


def get_people_data(db, resolution=ONE_WEEK):
    total_series = []

    committers = set()
    last_time = None

    for commit in db.query('SELECT email, date FROM commits WHERE valid==1 ORDER BY date'):
        email = get_canonical_email(commit['email'])
        dt = get_datetime_from_dict(commit, 'date')
        committers.add(email)

        if last_time is None or dt - last_time > resolution:
            last_time = dt
            total_series.append((dt, len(committers)))

    return total_series
