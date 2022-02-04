import re

from .constants import architectures, os_list


APT_REPO_PREFIX = re.compile(r'^(ros[\-a-z23]*)/+(ubuntu|rhel)/(.*)')
distro = r'(' + '|'.join(os_list) + ')'
DISTRO_PREFIX = re.compile(r'^dists/' + distro + r'/(.*)')
POOL_PREFIX = re.compile(r'^pool/main/./[^/]+/(.*)')
RHEL_PREFIX = re.compile(r'8/x86_64/Packages/./(.*)')
arches = '|'.join(architectures)
arch = r'(?:binary-(' + arches + ')|(source))'
short_arch = r'(' + arches + '|all)'
version = r'([\d\-\.~]+)'
ROS_PKG_PREFIX = re.compile(r'^ros\-(\w+)\-([^_]+)_' + version + distro + '(.*)')
ROS_RPM_PREFIX = re.compile(r'^ros\-(\w+)\-([\w\-]+)\-' + version + r'\.(el8)\.(.*)')
ARCH_LIBRARY_PREFIX = re.compile(r'([^_]+)_.+_' + short_arch + '(.*)$')
LIBRARY_PREFIX = re.compile(r'([^_]+)_' + version + r'(\..*)$')

PACKAGE_SUFFIX = re.compile(r'([^/]*\.deb|\.dsc|\.debian\.tar\.[xg]z|\.orig\.tar\.gz|[^/]+\.rpm)$')

ARCH_INFO = re.compile(r'main/' + arch + '/(Packages(.gz)?|Release|Sources.gz)$')
META_INFO = re.compile(r'(In)?Release(\.gpg)?$')

MISC = ['Others', 'ros.key', 'ros.asc']


def classify_link(s):
    if s in MISC:
        return {'misc': s}

    data = {}
    m = APT_REPO_PREFIX.match(s)
    if not m:
        return
    data['apt_repo'], os, s = m.groups()

    m = DISTRO_PREFIX.match(s)
    if m:
        data['distro'], s = m.groups()
        m = ARCH_INFO.match(s)
        if m:
            data['arch'] = m.group(1)
            return data
        elif META_INFO.match(s):
            return data
        else:
            return None
    m = POOL_PREFIX.match(s)
    if m:
        s = m.group(1)
    else:
        m = RHEL_PREFIX.match(s)
        if not m:
            return None
        s = m.group(1)

    s = m.group(1)
    m1 = ROS_PKG_PREFIX.match(s)
    m2 = ROS_RPM_PREFIX.match(s)
    if m1 or m2:
        m = m1 or m2
        # Ignore the version
        data['rosdistro'], data['package'], version, data['distro'], s = m.groups()
        if PACKAGE_SUFFIX.match(s):
            return data
        else:
            return None

    m = ARCH_LIBRARY_PREFIX.match(s)
    if m:
        data['library'], data['short_arch'], s = m.groups()
        if PACKAGE_SUFFIX.match(s):
            return data
    else:
        m = LIBRARY_PREFIX.match(s)
        if m:
            # Ignore the version
            data['library'], version, s = m.groups()
            if PACKAGE_SUFFIX.match(s):
                return data
