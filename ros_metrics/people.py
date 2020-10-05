import re
import yaml

# This data associates multiple aliases with a single person entity.
# We use a (semi-arbitrarily chosen) single email as the unique identifier, which we call the canonical email.
# There can multiple emails associated with the canonical email, as well as a person's given name, a Github account,
# a discourse user id and an answers.ros.org profile.

GITHUB_SUFFIX = '@users.noreply.github.com'
GITHUB_PATTERN = re.compile(r'(.*)' + GITHUB_SUFFIX)

# People data is stored as a yaml for convenient PR changes
PEOPLE_DATA_PATH = 'data/people.yaml'
PEOPLE_DATA = yaml.safe_load(open(PEOPLE_DATA_PATH))
TO_CANONICAL = {}

for email, person_dict in PEOPLE_DATA.items():
    for alt_email in person_dict.get('alt_emails', []):
        TO_CANONICAL[alt_email.lower()] = email

    if 'github' in person_dict:
        # Map from github id to canonical email, as well as generated email from github id
        gh_lower = person_dict['github'].lower()
        TO_CANONICAL[gh_lower] = email
        TO_CANONICAL[gh_lower + GITHUB_SUFFIX] = email


def is_github_email(email):
    m = GITHUB_PATTERN.match(email)
    if m:
        return m.group(1)


def get_canonical_email(email):
    email = email.lower()
    return TO_CANONICAL.get(email, email)


def get_name(email):
    email = get_canonical_email(email)
    return PEOPLE_DATA.get(email, {}).get('name', email)
