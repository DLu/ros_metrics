#!/usr/bin/env python3

import argparse
import collections
import pathlib

from jinja2 import Environment, FileSystemLoader

from ros_metrics import analytics, answers, packages, repos, wiki
from ros_metrics import charts, tables
from ros_metrics.constants import distros, os_list
from ros_metrics.metric_db import MetricDB

from tqdm import tqdm

parser = argparse.ArgumentParser()
parser.add_argument('-f', '--filter')
args = parser.parse_args()

OUTPUT_FOLDER = pathlib.Path('docs')

STRUCTURE = [
    {'name': 'Users',
     'filename': 'index.html',
     'chart': charts.get_users_plot,
     'caption': 'A collection of different metrics for measuring the number of users in the ROS community.'
     },
    {'name': 'Packages',
     'subpages': [
         {'name': 'Top',
          'template': 'top.html',
          'tops': packages.top_report,
          'caption': 'Most downloaded packages via packages.ros.org, '
                     'and the most downloaded packages introduced in each year.'},
         {'name': 'ROS Distro',
          'chart': charts.get_package_ratio_chart,
          'args': ['rosdistro', 'ROS Distro Usage by packages.ros.org traffic', distros],
          'caption': 'Relative usage of each distro based on downloads from packages.ros.org. '
                     'Note that data after late 2018 is not complete.'
          },
         {'name': 'Architecture',
          'chart': charts.get_package_ratio_chart,
          'args': ['arch', 'Architecture Usage by packages.ros.org traffic'],
          'caption': 'Chip architecture usage by packages.ros.org downloads.'
          },
         {'name': 'AptRepo',
          'chart': charts.get_package_ratio_chart,
          'args': ['apt_repo', 'Apt-repo Usage by packages.ros.org traffic'],
          'caption': 'Apt-repo usage based on packages.ros.org downloads.'
          },
         {'name': 'Linux',
          'chart': charts.get_package_ratio_chart,
          'args': ['distro', 'Linux Distro Usage by packages.ros.org traffic', os_list, 0.5],
          'caption': 'Linux distro usage based on packages.ros.org downloads.'
          },
         # {'name': 'Library',
         #  'chart': get_package_ratio_chart(dbs['packages'], 'library')
         #  },
         {'name': 'Country',
          'template': 'zing_chart.html',
          'chart': charts.get_package_country_chart,
          'caption': 'Top ROS-using countries based on packages.ros.org downloads.'
          },
         {'name': 'ROS2',
          'chart': charts.get_binaries_chart,
          'caption': 'Number of binary downloads per ROS2 release, broken down by OS.'
          }
         # {'name': 'OS',
         # 'chart': charts.get_package_os_chart(dbs['packages'])
         # }
     ]
     },
    {'name': 'ROS Distro',
     'subpages': [
         # {'name': 'Classification',
         # 'chart': charts.get_rosdistro_plot},
         {'name': 'ROS Distro',
          'chart': charts.get_rosdistro_distros,
          'caption': 'Relative maintenance of ROS distros by commits to ros/rosdistro.'},
         {'name': 'Verbs',
          'chart': charts.get_rosdistro_verbs,
          'caption': 'Commits to ros/rosdistro by action-type'},
         {'name': 'Version',
          'chart': charts.get_rosdistro_versions,
          'caption': 'Types of version bumps, by ros/rosdistro commits.'},
         {'name': 'Deps',
          'chart': charts.get_rosdistro_deps,
          'caption': 'Types of dependencies added, measured by commits to ros/rosdistro.'},
         {'name': 'Number of Repos',
          'chart': charts.get_rosdistro_repos,
          'caption': 'Number of repos in ros/rosdistro'
          },
     ]
     },
    {'name': 'Repos',
     'subpages': [
         {'name': 'Table',
          'template': 'table.html',
          'table': tables.github_repos_report,
          'caption': 'Github repos listed in ros/rosdistro, ranked by number of forks/stars/subscriptions'
          },
         {'name': 'Total Backlog',
          'chart': charts.get_repo_issues},
         {'name': 'Issues and PRs',
          'chart': charts.get_ticket_totals},
     ]
     },
    {'name': 'Answers',
     'subpages': [
         {'name': 'Questions',
          'chart': charts.get_questions_plot,
          'caption': 'Total number of questions, answers, questions with an accepted answer on answers.ros.org, '
                     'and the percent of questions with an accepted answer.'},
         {'name': 'Karma',
          'chart': charts.get_karma_chart,
          'caption': 'Distribution of karma among answers.ros.org users.',
          },
         {'name': 'ROS Distro',
          'chart': charts.get_answers_distro_chart,
          'caption': 'Relative usage of ROS distros by question tags on answers.ros.org.'
          },
         {'name': 'Top Answerers',
          'template': 'top.html',
          'tops': answers.get_top_users,
          'caption': 'Top answerers of questions on answers.ros.org, overall and by year.'
          },
         {'name': 'Interesting Qs',
          'template': 'top.html',
          'tops': answers.get_top_questions,
          'caption': 'Various "interesting" statistics'
          }
     ]
     },
    {'name': 'Analytics',
     'subpages': [
         {'name': 'Pageviews',
          'chart': charts.get_analytics_totals_chart,
          'caption': 'Pageviews to key ROS websites as reported by Google Analytics '
                     '(and Apache logs for packages.ros.org)'
          },
         {'name': 'Users',
          'chart': charts.get_analytics_totals_chart,
          'args': ['users', False],
          'caption': 'Users to key ROS websites as reported by Google Analytics'
          },
         {'name': 'Sessions',
          'chart': charts.get_analytics_totals_chart,
          'args': ['sessions', False],
          'caption': 'Number of sessions to key ROS websites as reported by Google Analytics'
          },
         # {'name': 'By Country',
         # 'chart': charts.get_analytics_country_chart(dbs['analytics'])
         # }
     ]
     },
    {'name': 'Wiki',
     'subpages': [
         {'name': 'Activity',
          'chart': charts.get_wiki_chart,
          'caption': 'Total number of pages and edits for wiki.ros.org'
          },
         {'name': 'TopPages',
          'template': 'top.html',
          'tops': analytics.top_wiki_report,
          'caption': 'Top wiki pages, as measured by Google Analytics, both overall and by year created.'
          },
         {'name': 'Interesting',
          'template': 'top.html',
          'tops': wiki.interesting_wiki_report,
          'caption': 'Interesting factoids for the wiki.'
          },
     ]},
    {'name': 'Misc',
     'subpages': [
         {'name': 'Emails',
          'chart': charts.get_emails_plot,
          'caption': 'Number of posts/threads on the two email platforms. '
                     'Note that answers.ros.org was introduced in early 2011.'
          },
         {'name': 'Citations',
          'chart': charts.get_scholar_plot,
          'caption': 'Number of citations to "ROS: an open-source Robot Operating System"'
          }
     ]
     },
]


j2_env = Environment(loader=FileSystemLoader('viz/templates'))
OUTPUT_FOLDER.mkdir(exist_ok=True)

menu = collections.OrderedDict()
for blob in STRUCTURE:
    name = blob['name']
    if 'subpages' not in blob:
        if 'filename' not in blob:
            filename = name.lower() + '.html'
            blob['filename'] = filename
        else:
            filename = blob['filename']
        menu[filename] = name
    else:
        submenu = collections.OrderedDict()
        for subpage in blob['subpages']:
            if 'filename' not in subpage:
                filename = name.lower().replace(' ', '') + '_' + subpage['name'].lower().replace(' ', '') + '.html'
                subpage['filename'] = filename
            else:
                filename = subpage['filename']
            submenu[filename] = subpage['name']
        blob['submenu'] = submenu
        chosen = blob['subpages'][0]['filename']
        menu[chosen] = blob['name']
        blob['filename'] = chosen


for blob in STRUCTURE:
    level1 = blob['filename']
    if 'subpages' not in blob:
        template = j2_env.get_template(blob.get('template', 'basic_chart.html'))
        blob['level1'] = level1
        blob['menu'] = menu
        if args.filter and args.filter not in level1:
            continue
        print(level1)
        for key, value in blob.items():
            if hasattr(value, '__call__'):
                blob[key] = value(*blob.get('args', []))
        with open(OUTPUT_FOLDER / level1, 'w') as f:
            f.write(template.render(**blob))
    else:
        for subpage in blob['subpages']:
            template = j2_env.get_template(subpage.get('template', 'basic_chart.html'))
            level2 = subpage['filename']
            if args.filter and args.filter not in level2:
                continue
            print(level2)

            for key, value in subpage.items():
                if hasattr(value, '__call__'):
                    subpage[key] = value(*subpage.get('args', []))
            subpage['level1'] = level1
            subpage['level2'] = level2
            subpage['menu'] = menu
            subpage['submenu'] = blob['submenu']
            with open(OUTPUT_FOLDER / level2, 'w') as f:
                f.write(template.render(**subpage))

if args.filter and args.filter != 'repos':
    exit(0)

rosdistro_db = MetricDB('rosdistro')
repos_db = MetricDB('repos')
github_repos = repos.get_github_repos(rosdistro_db)
REPOS_FOLDER = OUTPUT_FOLDER / 'repos'
REPOS_FOLDER.mkdir(exist_ok=True)
for id, repo_dict in tqdm(github_repos.items()):
    name = '{org}/{repo}'.format(**repo_dict)
    template = j2_env.get_template(subpage.get('template', 'basic_chart.html'))
    filename = '{org}_{repo}.html'.format(**repo_dict)

    repo_page = {'name': name, 'chart': charts.get_repo_issues(repos_db, name, id)}

    repo_page['level1'] = 'repos.html'
    repo_page['menu'] = menu
    repo_page['prefix'] = '../'
    with open(REPOS_FOLDER / filename, 'w') as f:
        f.write(template.render(**repo_page))
