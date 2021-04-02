import io
import pathlib
import re

import git

import requests

GITHUB_HTTP_PATTERN = re.compile(r'https?://(?P<server>github\.com)/(?P<org>[^/]+)/(?P<repo>.+?)(?:\.git)?$')
GITHUB_SSH_PATTERN = re.compile(r'git@(?P<server>github\.com):(?P<org>[^/]+)/(?P<repo>.+)\.git')
GITHUB_URI_PATTERN = re.compile(r'git://(?P<server>github\.com)/(?P<org>[^/]+)/(?P<repo>.+?)(?:\.git)?$')
BB_PATTERN = re.compile(r'https://(?P<server>bitbucket\.org)/(?P<org>.*)/(?P<repo>.+)')
GITLAB_HTTP_PATTERN = re.compile(r'https?://(?P<server>gitlab\.[^/]+)/(?P<org>[^/]+)/(?P<repo>.+).git')
GITLAB_SSH_PATTERN = re.compile(r'git@(?P<server>gitlab\.[^/]+):(?P<org>[^/]+)/(?P<repo>.+)\.git')
GOOGLECODE_PATTERN = re.compile(r'https?://(?P<org>[^\.]+)\.(?P<server>googlecode.com)/svn/.*/(?P<repo>.+)')
KFORGE_PATTERN = re.compile(r'https?://(?P<server>kforge.ros.org)/(?P<org>[^/]+)/(?P<repo>.+)')
CODEROS_PATTERN = re.compile(r'https?://(?P<server>code.ros.org)/svn/(?P<org>[^/]+)/stacks/(?P<repo>.+)/trunk')
CODEROS_BRANCH_PATTERN = re.compile(r'https?://(?P<server>code.ros.org)/svn/(?P<org>[^/]+)/stacks/(?P<repo>.+)/'
                                    'branches/(?P<branch>.+)')
SF_PATTERN = re.compile(r'https?://svn.(?P<server>code.sf.net)/p/(?P<org>[^/]+)/code/trunk/(?:stacks/)?(?P<repo>.+)')

PATTERNS = [GITHUB_HTTP_PATTERN, GITHUB_SSH_PATTERN, GITHUB_URI_PATTERN, BB_PATTERN, GITLAB_HTTP_PATTERN,
            GITLAB_SSH_PATTERN, GOOGLECODE_PATTERN, KFORGE_PATTERN, CODEROS_PATTERN, CODEROS_BRANCH_PATTERN, SF_PATTERN]

REPOS_CACHE_PATH = pathlib.Path('cache/repos')


def match_git_host(url):
    if not url:
        return
    for pattern in PATTERNS:
        m = pattern.match(url)
        if m:
            return {k: v.lower() for (k, v) in m.groupdict().items()}


def get_cache_folder(repo_dict):
    return REPOS_CACHE_PATH / repo_dict['org'] / repo_dict['repo']


class CloneException(Exception):
    def __init__(self, message):
        self.message = message


CLONE_MESSAGES = [
    ('mercurial', 'Mercurial (hg) is required'),
    ('no_access', 'HTTP Basic: Access denied'),
    ('no_access', 'Permission denied'),
    ('no_access', 'Connection refused'),
    ('missing', 'not found'),
]


def clone_or_update(url, path=None, update=True):
    if path is None:
        repo_dict = match_git_host(url)
        path = get_cache_folder(repo_dict)

    if not path.exists():
        try:
            repo = git.Repo.clone_from(url, path)
        except git.GitCommandError as e:
            if not url.startswith('git@'):
                r = requests.get(url, timeout=3.0)
                if r.status_code == 404:
                    raise CloneException('missing')

            for status, msg in CLONE_MESSAGES:
                if msg in e.stderr:
                    raise CloneException(status)
            print(e)
            exit(0)
    else:
        repo = git.Repo(path)
        if update:
            repo.remotes.origin.pull()
    return repo, path


def resolve(url):
    if url.startswith('git@'):
        return url

    # Special case for some historical repos
    m = GITHUB_URI_PATTERN.match(url)
    if m:
        url = 'https://github.com/{org}/{repo}.git'.format(**m.groupdict())

    original = url
    try:
        while True:
            r = requests.get(url, allow_redirects=False, timeout=3.0)
            if r.status_code != 301:
                break
            url = r.headers['Location']
    except requests.exceptions.ConnectTimeout:
        raise
    except Exception as e:
        print(url, e)
        return None

    if original.endswith('.git') and not url.endswith('.git'):
        url += '.git'
    if not original.endswith('/') and url.endswith('/'):
        url = url[:-1]

    return url


def blob_contents(blob):
    if blob is None:
        return ''
    s = ''
    with io.BytesIO(blob.data_stream.read()) as f:
        s += f.read().decode('utf-8')
    return s


def tree_iterator(tree, filename_filter=None, subfolder=pathlib.Path()):
    for subtree in sorted(tree.trees, key=lambda d: d.name):
        yield from tree_iterator(subtree, filename_filter, subfolder / subtree.name)
    for blob in tree.blobs:
        if filename_filter and filename_filter != blob.name:
            continue
        yield subfolder / blob.name, blob


def find_manifests(tree):
    yield from tree_iterator(tree, 'manifest.xml')
    yield from tree_iterator(tree, 'package.xml')
