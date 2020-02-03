import git
import io
import pathlib
import re
import requests

GITHUB_HTTP_PATTERN = re.compile('https?://(?P<server>github\.com)/(?P<org>[^/]+)/(?P<repo>.+?)(?:\.git)?$')
GITHUB_SSH_PATTERN = re.compile('git@(?P<server>github\.com):(?P<org>[^/]+)/(?P<repo>.+)\.git')
BB_PATTERN = re.compile('https://(?P<server>bitbucket\.org)/(?P<org>.*)/(?P<repo>.+)')
GITLAB_HTTP_PATTERN = re.compile('https?://(?P<server>gitlab\.[^/]+)/(?P<org>[^/]+)/(?P<repo>.+).git')
GITLAB_SSH_PATTERN = re.compile('git@(?P<server>gitlab\.[^/]+):(?P<org>[^/]+)/(?P<repo>.+)\.git')
GOOGLECODE_PATTERN = re.compile('https?://(?P<org>[^\.]+)\.(?P<server>googlecode.com)/svn/.*/(?P<repo>.+)')
KFORGE_PATTERN = re.compile('https?://(?P<server>kforge.ros.org)/(?P<org>[^/]+)/(?P<repo>.+)')
CODEROS_PATTERN = re.compile('https?://(?P<server>code.ros.org)/svn/(?P<org>[^/]+)/stacks/(?P<repo>.+)/trunk')
SF_PATTERN = re.compile('https?://svn.(?P<server>code.sf.net)/p/(?P<org>[^/]+)/code/trunk/(?:stacks/)?(?P<repo>.+)')

PATTERNS = [GITHUB_HTTP_PATTERN, GITHUB_SSH_PATTERN, BB_PATTERN, GITLAB_HTTP_PATTERN, GITLAB_SSH_PATTERN,
            GOOGLECODE_PATTERN, KFORGE_PATTERN, CODEROS_PATTERN, SF_PATTERN]

REPOS_CACHE_PATH = pathlib.Path('cache/repos')


def match_git_host(url):
    if not url:
        return
    for pattern in PATTERNS:
        m = pattern.match(url)
        if m:
            return dict([(k, v.lower()) for (k, v) in m.groupdict().items()])


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

def blob_contents(blob):
    if blob is None:
        return ''
    s = ''
    with io.BytesIO(blob.data_stream.read()) as f:
        s += f.read().decode('utf-8')
    return s
