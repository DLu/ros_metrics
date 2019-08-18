from .rosdistro import get_rosdistro_repo, REPO_PATH
from .constants import distros
import pathlib
import git
import yaml
import collections
from tqdm import tqdm

CACHE_PATH = pathlib.Path('cache/repos')
FORBIDDEN_KEYS = ['-release', 'ros.org', 'svn']


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


def get_repo_list():
    all_repos = get_raw_distro_dict()
    repo_list = []
    for name, repo_dict in all_repos.items():
        urls = list(set(repo_dict.values()))
        if len(urls) == 1:
            for key in FORBIDDEN_KEYS:
                if key in urls[0]:
                    break
            else:
                repo_list.append(urls[0])
        elif two_substring_match(urls, 'ros2'):
            repo_list += urls
        else:
            for key in FORBIDDEN_KEYS:
                x = two_substring_match(urls, key)
                if x:
                    repo_list.append(x)
                    break
            else:
                # TODO: 404, 301 resolution
                pass
    return repo_list


def clone(url_list, debug=False):
    repos = []
    to_clone = []

    CACHE_PATH.mkdir(exist_ok=True)
    for url in url_list:
        p = pathlib.Path(url)
        folder = CACHE_PATH / p.stem
        if folder.exists():
            repos.append(git.Repo(str(folder)))
        else:
            to_clone.append((folder, url))
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


def update_repos():
    urls = get_repo_list()
    repos = clone(urls)
    update(repos)
    # https://developer.github.com/v3/
    # forks_count/network_count/stargazers_count/subscribers_count/watchers_count/get_contributors()/ get_languages()
