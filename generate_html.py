import collections
from jinja2 import Environment, FileSystemLoader
import pathlib
from ros_metrics import charts
from ros_metrics.metric_db import MetricDB
from ros_metrics import analytics, answers, packages
from ros_metrics.constants import distros, os_list

OUTPUT_FOLDER = pathlib.Path('docs')

dbs = {}
for name in ['discourse', 'answers', 'ros_users', 'packages', 'scholar', 'rosdistro', 'analytics']:
    dbs[name] = MetricDB(name)

STRUCTURE = [
    {'name': 'Users',
     'filename': 'index.html',
     'chart': charts.get_users_plot(dbs['discourse'], dbs['answers'], dbs['ros_users'], dbs['rosdistro']),
     'caption': 'A collection of different metrics for measuring the number of users in the ROS community.'
     },
    {'name': 'Packages',
     'subpages': [
         {'name': 'Top',
          'template': 'top.html',
          'tops': packages.top_report(dbs['packages']),
          'caption': 'Most downloaded packages via packages.ros.org, '
                     'and the most downloaded packages introduced in each year.'},
         {'name': 'ROS Distro',
          'chart': charts.get_package_ratio_chart(dbs['packages'], 'rosdistro', distros),
          'caption': 'Relative usage of each distro based on downloads from packages.ros.org. '
                     'Note that data after late 2018 is not complete.'
          },
         {'name': 'Architecture',
          'chart': charts.get_package_ratio_chart(dbs['packages'], 'arch'),
          'caption': 'Chip architecture usage by packages.ros.org downloads.'
          },
         {'name': 'AptRepo',
          'chart': charts.get_package_ratio_chart(dbs['packages'], 'apt_repo'),
          'caption': 'Apt-repo usage based on packages.ros.org downloads.'
          },
         {'name': 'Linux',
          'chart': charts.get_package_ratio_chart(dbs['packages'], 'distro', os_list, 0.5),
          'caption': 'Linux distro usage based on packages.ros.org downloads.'
          },
         # {'name': 'Library',
         #  'chart': get_package_ratio_chart(dbs['packages'], 'library')
         #  },
         {'name': 'Country',
          'template': 'countries.html',
          'chart': charts.get_package_country_chart(dbs['packages']),
          'rankings': charts.get_package_country_list(dbs['packages']),
          'caption': 'Top ROS-using countries based on packages.ros.org downloads.'
          },
         # {'name': 'OS',
         # 'chart': charts.get_package_os_chart(dbs['packages'])
         # }
     ]
     },
    {'name': 'ROS Distro',
     'subpages': [
         # {'name': 'Classification',
         # 'chart': charts.get_rosdistro_plot(dbs['rosdistro'])},
         {'name': 'ROS Distro',
          'chart': charts.get_rosdistro_distros(dbs['rosdistro']),
          'caption': 'Relative usage of ROS distros by commits to ros/rosdistro.'},
         {'name': 'Verbs',
          'chart': charts.get_rosdistro_verbs(dbs['rosdistro']),
          'caption': 'Commits to ros/rosdistro by action-type'},
         {'name': 'Version',
          'chart': charts.get_rosdistro_versions(dbs['rosdistro']),
          'caption': 'Types of version bumps, by ros/rosdistro commits.'},
         {'name': 'Deps',
          'chart': charts.get_rosdistro_deps(dbs['rosdistro']),
          'caption': 'Types of dependencies added, measured by commits to ros/rosdistro.'},
     ]
     },
    {'name': 'Answers',
     'subpages': [
         {'name': 'Questions',
          'chart': charts.get_questions_plot(dbs['answers']),
          'caption': 'Total number of questions, answers, questions with an accepted answer on answers.ros.org, '
                     'and the percent of questions with an accepted answer.'},
         {'name': 'Karma',
          'chart': charts.get_karma_chart(dbs['answers']),
          'caption': 'Distribution of karma among answers.ros.org users.',
          },
         {'name': 'ROS Distro',
          'chart': charts.get_answers_distro_chart(dbs['answers']),
          'caption': 'Relative usage of ROS distros by question tags on answers.ros.org.'
          },
         {'name': 'Top Answerers',
          'template': 'top.html',
          'tops': answers.get_top_users(dbs['answers']),
          'caption': 'Top answerers of questions on answers.ros.org, overall and by year.'
          }
     ]
     },
    {'name': 'Misc',
     'subpages': [
         {'name': 'Analytics',
          'chart': charts.get_analytics_totals_chart(dbs['analytics'], dbs['packages']),
          'caption': 'Relative traffic to key ROS websites as reported by Google Analytics '
                     '(and Apache logs for packages.ros.org)'
          },
         # {'name': 'By Country',
         # 'chart': charts.get_analytics_country_chart(dbs['analytics'])
         # }
         {'name': 'Wiki',
          'template': 'top.html',
          'tops': analytics.top_wiki_report(dbs['analytics']),
          'caption': 'Top wiki pages, as measured by Google Analytics, both overall and by year created.'
          },
         {'name': 'Emails',
          'chart': charts.get_emails_plot(dbs['discourse'], dbs['ros_users']),
          'caption': 'Number of posts/threads on the two email platforms. '
                     'Note that answers.ros.org was introduced in early 2011.'
          },
         {'name': 'Citations',
          'chart': charts.get_scholar_plot(dbs['scholar']),
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
            filename = name.lower() + '_' + subpage['name'].lower().replace(' ', '') + '.html'
            subpage['filename'] = filename
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
        print(level1)
        with open(OUTPUT_FOLDER / level1, 'w') as f:
            f.write(template.render(**blob))
    else:
        for subpage in blob['subpages']:
            template = j2_env.get_template(subpage.get('template', 'basic_chart.html'))
            level2 = subpage['filename']
            print(level2)
            subpage['level1'] = level1
            subpage['level2'] = level2
            subpage['menu'] = menu
            subpage['submenu'] = blob['submenu']
            with open(OUTPUT_FOLDER / level2, 'w') as f:
                f.write(template.render(**subpage))
