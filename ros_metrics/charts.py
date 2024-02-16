import collections
import copy
import datetime

import yaml

from . import analytics, answers, binaries, commits, packages, repos, rosdistro, scholar
from .constants import countries, distros
from .metric_db import MetricDB
from .reports import buckets_to_plot, get_email_plots, get_regular_aggregate_series, get_datetime_from_dict
from .reports import get_regular_unique_series, get_series, normalize_timepoints, round_series, time_buckets
from .util import VERSIONS, epoch_to_datetime, get_manual_stats, year_month_to_datetime

BASIC_TIME_OPTIONS = {
    'responsive': True,
    'scales': {
        'xAxes': [{'type': 'time', 'display': True}]
    },
    'tooltips': {
        'mode': 'xfirst',
        'intersect': False
    }
}

STACKED_BAR_OPTIONS = {
    'responsive': True,
    'scales': {
        'xAxes': [{'type': 'time', 'display': True, 'stacked': True, 'barPercentage': 1.0, 'categoryPercentage': 1.0}],
        'yAxes': [{'stacked': True, 'ticks': {'max': 1.0}}]
    }
}

DEFINED_COLORS = yaml.safe_load(open('viz/colors.yaml'))
SOME_COLORS = ['#4dc9f6',
               '#f67019',
               '#f53794',
               '#537bc4',
               '#acc236',
               '#166a8f',
               '#00a950',
               '#58595b',
               '#8549ba',
               '#a03232',
               '#ffeb3b']


class BaseChart(dict):
    def __init__(self, chart_type, options=None):
        self['type'] = chart_type

        if options is not None:
            self['options'] = copy.deepcopy(options)
        else:
            self['options'] = {}


class Chart(BaseChart):
    def __init__(self, chart_type, options=None, title=None):
        BaseChart.__init__(self, chart_type, options if options is not None else BASIC_TIME_OPTIONS)
        self['data'] = {'datasets': []}

        if title:
            self['options']['title'] = {
                'display': True,
                'text': title,
                'fontSize': 24,
            }
        self.color_i = 0

    def add(self, name, series, color=None, **kwargs):
        if series and isinstance(series[0], tuple):
            new_series = []
            for x, y in series:
                if isinstance(x, datetime.datetime):
                    x = x.isoformat()
                new_series.append({'x': x, 'y': y})

            # If all points in series are at midnight, cleave the time spec
            if isinstance(series[0][0], datetime.datetime) and all('T00:00:00' in d['x'] for d in new_series):
                for d in new_series:
                    d['x'] = d['x'].replace('T00:00:00', '')
            series = new_series

        if color is None:
            if name in DEFINED_COLORS:
                color = DEFINED_COLORS[name]
            else:
                color = SOME_COLORS[self.color_i % len(SOME_COLORS)]
                self.color_i += 1

        self.add_dataset(name, series, color, **kwargs)

    def add_dataset(self, name, series, color=None, **kwargs):
        data_dict = {'label': name, 'data': series}
        if color:
            data_dict['backgroundColor'] = color
        if self['type'] == 'line' and 'fill' not in kwargs:
            data_dict['fill'] = False
        data_dict.update(kwargs)
        self['data']['datasets'].append(data_dict)


class ZingChart(dict):
    def __init__(self, chart_type, options=None, title=None):
        BaseChart.__init__(self, chart_type, options)
        self['series'] = []
        self['globals'] = {'fontFamily': 'Overpass, sans-serif'}

    def add_dataset(self, name, series, color=None, **kwargs):
        data_dict = {'text': name, 'values': series}
        if color:
            data_dict['backgroundColor'] = color
        data_dict.update(kwargs)
        self['series'].append(data_dict)


def bucket_plot(buckets, values=None, other_limit=None, title=None):
    chart = Chart('bar', STACKED_BAR_OPTIONS, title=title)
    other = collections.Counter()
    for name, d_series in normalize_timepoints(buckets, values).items():
        if other_limit is not None:
            total_area = sum(x[1] for x in d_series)
            if total_area < other_limit:
                for x, y in d_series:
                    other[x] += y
                continue
        chart.add(name, d_series)

    if other:
        chart.add('other', sorted(other.items()))
    return chart


def get_users_plot():
    discourse_db = MetricDB('discourse')
    answers_db = MetricDB('answers')
    users_db = MetricDB('ros_users')
    rosdistro_db = MetricDB('rosdistro')
    stack_db = MetricDB('stack_exchange')
    wiki_db = MetricDB('wiki')
    commits_db = MetricDB('commits')
    chart = Chart('line', title='Number of ROS Users')

    manual = get_manual_stats('users subscribers')
    chart.add('ros-users subscribers', sorted(manual.items()), color='#4dc9f6')
    chart.add('ros-users posters',
              get_regular_unique_series(users_db, 'posts', 'created_at', 'user_id'), color='#9ee1fa')

    manual_wiki = get_manual_stats('wiki.ros.org users')
    chart.add('wiki.ros.org users', sorted(manual_wiki.items()), color='#f67019')
    chart.add('wiki.ros.org editors', get_regular_unique_series(wiki_db, 'revisions', 'date', 'user'), color='#f9aa76')

    chart.add('answers.ros.org users', get_regular_aggregate_series(answers_db, 'users', 'created_at'), color='#f53794')
    chart.add('answers.ros.org questioners',
              get_regular_unique_series(answers_db, 'questions', 'created_at', 'user_id'), color='#f98bc0')
    chart.add('answers.ros.org answerers',
              get_regular_unique_series(answers_db, 'answers', 'created_at', 'user_id'), color='#e90c77')

    chart.add('robotics.stackexchange.com ros users',
              get_regular_aggregate_series(stack_db, 'users', 'created_at'), color='#537bc4')

    total, active = rosdistro.get_people_data(rosdistro_db, None)
    chart.add('rosdistro committers', round_series(total), color='#acc236')

    chart.add('repo committers', round_series(commits.get_people_data(commits_db)), color='#166a8f')

    chart.add('Discourse users', get_regular_aggregate_series(discourse_db, 'users', 'created_at'), color='#00a950')
    chart.add('Discourse posters',
              get_regular_unique_series(discourse_db, 'posts', 'created_at', 'user_id'), color='#0aff7c')

    return chart


def get_emails_plot():
    discourse_db = MetricDB('discourse')
    users_db = MetricDB('ros_users')
    r_total, r_unique = get_email_plots(users_db)
    d_total, d_unique = get_email_plots(discourse_db)

    chart = Chart('line', title='Emails Per Month')
    chart.add('ros-users Posts', buckets_to_plot(r_total))
    chart.add('ros-users Topics', buckets_to_plot(r_unique))

    chart.add('Discourse Posts', buckets_to_plot(d_total))
    chart.add('Discourse Topics', buckets_to_plot(d_unique))
    return chart


def get_package_ratio_chart(field, title=None, values=None, other_limit=None):
    packages_db = MetricDB('packages')
    return bucket_plot(packages.package_ratios(packages_db, field, values), values=values, other_limit=other_limit,
                       title=title)


def get_package_country_chart(N=15):
    packages_db = MetricDB('packages')
    cc_list, by_country = packages.get_package_buckets(packages_db, 'countries', 'cc')
    buckets = time_buckets(packages_db, 'countries', cc_list, 'year, month', 'cc', 'hits', months=False)

    options = {'style': {'labelOverall': {'text': 'Overall'}}}

    chart = ZingChart('rankflow', options, title='Countries with most traffic to packages.ros.org')
    ranks = collections.defaultdict(dict)

    for year, c_dict in buckets.items():
        for i, (cc, _) in enumerate(c_dict.most_common(N)):
            ranks[year][cc] = i + 1

    for cc in cc_list:
        series = []
        valid = False
        for year in buckets:
            if cc in ranks[year]:
                series.append(ranks[year][cc])
                valid = True
            else:
                series.append(None)

        if valid:
            title = '{name} {emoji}'.format(**countries[cc.lower()])
            chart['series'].append({'text': title, 'ranks': series, 'rank': len(chart['series']) + 1})

    year_str = list(map(str, buckets.keys()))
    chart['scaleX'] = {
        'values': year_str,
        'labels': year_str
    }
    return chart


def get_package_os_chart():
    packages_db = MetricDB('packages')
    cc_list, buckets = packages.get_package_buckets(packages_db, 'os', 'os')
    chart = Chart('bar', STACKED_BAR_OPTIONS)
    for name, d_series in buckets.items():
        chart.add(name, d_series)
    return chart


def get_scholar_plot():
    scholar_db = MetricDB('scholar')

    scholar_options = copy.deepcopy(BASIC_TIME_OPTIONS)
    scholar_options['scales']['xAxes'][0]['time'] = {'unit': 'year'}
    scholar_options['scales']['yAxes'] = [{'type': 'logarithmic', 'ticks': {'callback': 'y_display'}}]

    chart = Chart('line', scholar_options, title='Citations')
    for title, series in scholar.get_report(scholar_db).items():
        chart.add(title, series)
    return chart


def merge_series_q(data):
    all_keys = set()
    for name in data:
        all_keys |= set(data[name].keys())

    prev = {}
    total_s = []
    for key in sorted(all_keys):
        total = 0
        for name in data:
            if key in data[name]:
                value = data[name][key]
                total += value
                prev[name] = value
            elif name in prev:
                total += prev[name]
        total_s.append((key, total))
    return total_s


def answered_report(db, extra_clause=''):
    data = {}
    answered = 0
    closed = 0
    total_q = 0

    clause = ''
    if extra_clause:
        clause = f'WHERE TRUE {extra_clause}'
    for q_dict in db.query(f'SELECT created_at, accepted_answer_id FROM questions {clause} ORDER BY created_at, id'):
        if q_dict['created_at'] is None:
            continue
        total_q += 1
        dt = get_datetime_from_dict(q_dict, 'created_at')

        accepted = q_dict['accepted_answer_id']
        if accepted is None:
            pass
        elif accepted < 0:
            closed += 1
        else:
            answered += 1

        data[dt] = {
            'total': total_q,
            'answered': answered,
            'closed': closed,
        }
    return data


def get_question_sites():
    answers_db = MetricDB('answers')
    stack_db = MetricDB('stack_exchange')
    return [
        ('answers.ros.org', answers_db, ''),
        ('Robotics Stack Exchange', stack_db, 'AND ros_id IS NULL'),
    ]


def get_questions_plot():
    chart = Chart('line', title='Q&A Overall Statistics')

    qdata = {}
    adata = {}
    pdata = {}
    queue = []
    for name, db, extra_clause in get_question_sites():
        question_series = get_regular_aggregate_series(db, 'questions', 'created_at', clause=extra_clause)
        queue.append((f'Total {name} Questions', question_series))
        qdata[name] = dict(question_series)

        answer_series = get_regular_aggregate_series(db, 'answers', 'created_at', clause=extra_clause)
        queue.append((f'Total {name} Answers', answer_series))
        adata[name] = dict(answer_series)

        answered_data = answered_report(db, extra_clause=extra_clause)
        pdata[name] = answered_data
        x = [(key, value['answered'] / value['total']) for (key, value) in sorted(answered_data.items())]
        queue.append((f'{name} Percent Answered', round_series(x)))

    chart.add('Total Questions', merge_series_q(qdata))
    chart.add('Total Answers', merge_series_q(adata))

    # Merge Answered Data
    all_keys = set()
    for name in pdata:
        all_keys |= set(pdata[name].keys())

    prev = {}
    answered_s = []
    closed_s = []
    ratio_s = []
    for key in sorted(all_keys):
        total = collections.Counter()

        for name in pdata:
            if key in pdata[name]:
                for field in pdata[name][key]:
                    value = pdata[name][key][field]
                    total[field] += value
                prev[name] = pdata[name][key]
            elif name in prev:
                for field in prev[name]:
                    total[field] += prev[name][field]

        answered_s.append((key, total['answered']))
        closed_s.append((key, total['closed']))
        ratio_s.append((key, total['answered'] / total['total']))
    chart.add('Percent Answered', round_series(ratio_s), yAxisID='percent')
    chart.add('Answered Questions', round_series(answered_s))
    chart.add('Closed Questions', round_series(closed_s))

    for name, plot in queue:
        if 'Percent' in name:
            chart.add(name, plot, yAxisID='percent')
        else:
            chart.add(name, plot)

    chart['options']['scales']['yAxes'] = [{'title': 'count'},
                                           {'id': 'percent', 'position': 'right', 'ticks': {'suggestedMin': 0}}]
    return chart


def get_karma_chart():
    answers_db = MetricDB('answers')
    karma = answers.karma_report(answers_db)

    chart = Chart('horizontalBar', {}, 'answers.ros.org Karma Distribution')
    chart['data']['labels'] = [k[0] for k in karma]
    chart.add('Number of users', [k[1] for k in karma])
    return chart


def get_answers_distro_chart():
    all_buckets = collections.defaultdict(collections.Counter)
    for name, db, extra_clause in get_question_sites():
        for prefix in ['', 'ros-']:
            values = [prefix + distro for distro in distros]
            buckets = time_buckets(db, 'questions INNER JOIN tags on tags.q_id = questions.id', values,
                                   'created_at', 'tag', extra_clause=extra_clause)
            for month, counts in buckets.items():
                for key, count in counts.items():
                    all_buckets[month][key.replace(prefix, '')] += count

    chart = Chart('bar', STACKED_BAR_OPTIONS, 'ROS Distro Usage by Q+A site tags')
    for name, d_series in normalize_timepoints(all_buckets, distros).items():
        chart.add(name, d_series)
    return chart


def get_analytics_totals_chart(metric='pageviews', packages=True):
    analytics_db = MetricDB('analytics')
    chart = Chart('line', title='Overall Traffic to ROS sites')
    for name, d_series in analytics.get_total_series(analytics_db, metric).items():
        chart.add(name, d_series)
    if packages:
        packages_db = MetricDB('packages')
        chart.add('packages.ros.org', get_series(packages_db, 'traffic', 'year, month', 'visitors'))
    return chart


def get_analytics_country_chart():
    analytics_db = MetricDB('analytics')
    chart = Chart('line')
    data = analytics.get_country_traffic(analytics_db)
    for cc, d_series in sorted(data.items(), key=lambda k: k[1][-1]['y'], reverse=True)[:10]:
        cc = cc.lower()
        title = '{name} {emoji}'.format(**countries[cc])
        chart.add(title, d_series)
    return chart


def get_rosdistro_plot():
    rosdistro_db = MetricDB('rosdistro')
    chart = Chart('line')
    chart.add('Known Types', rosdistro.get_classification_ratio(rosdistro_db))
    return chart


def get_rosdistro_verbs():
    rosdistro_db = MetricDB('rosdistro')
    return bucket_plot(rosdistro.get_verbs_ratio(rosdistro_db), title='Types of commits to rosdistro')


def get_rosdistro_distros():
    rosdistro_db = MetricDB('rosdistro')
    return bucket_plot(rosdistro.get_distro_action(rosdistro_db), distros,
                       title='ROS Distro Maintenance by rosdistro commits')


def get_rosdistro_versions():
    rosdistro_db = MetricDB('rosdistro')
    return bucket_plot(rosdistro.get_version_changes(rosdistro_db), VERSIONS + ['other'],
                       title='Major/Minor/Patch - Package Update Classifications')


def get_rosdistro_deps():
    rosdistro_db = MetricDB('rosdistro')
    return bucket_plot(rosdistro.get_dep_changes(rosdistro_db), other_limit=2.0, title='Types of Dependencies Added')


def get_rosdistro_people():
    rosdistro_db = MetricDB('rosdistro')
    chart = Chart('line')
    delta = datetime.timedelta(180)
    total, active = rosdistro.get_people_data(rosdistro_db, delta)
    chart.add('Total', total)
    chart.add('Active (six month window)', active)
    return chart


def get_rosdistro_repos():
    rosdistro_db = MetricDB('rosdistro')
    series = rosdistro.get_repo_report(rosdistro_db)
    chart = Chart('line', title='Number of Repositories in rosdistro')
    chart.add('All', round_series(series['all']))
    for distro in distros:
        if not series.get(distro):
            continue
        chart.add(distro, round_series(series[distro]))
    return chart


def get_repo_issues(repos_db=None, repo_name=None, repo_id=None, mode='month'):
    if repos_db is None:
        repos_db = MetricDB('repos')

    if repo_name is None:
        title = 'Overall Backlog Size'
        simplified = True
    else:
        title = f'{repo_name} Backlog Size'
        simplified = False

    chart = Chart('line', title=title)
    issues, prs = repos.get_issues_and_prs(repos_db, repo_id, simplified)
    chart.add('Open Issues', round_series(issues, mode), lineTension=0)
    chart.add('Open PRs', round_series(prs, mode), lineTension=0)
    return chart


def get_ticket_totals(repos_db=None):
    if repos_db is None:
        repos_db = MetricDB('repos')

    chart = Chart('line', title='')
    for key, line in repos.get_total_issues_and_prs(repos_db, simplified=True).items():
        chart.add(key, round_series(line), lineTension=0)
    return chart


def get_tarball_chart(binaries_db=None):
    if binaries_db is None:
        binaries_db = MetricDB('binaries')
    chart = Chart('bar', {
        'responsive': True,
        'scales': {
            'xAxes': [{'display': True, 'stacked': True, 'barPercentage': 1.0, 'categoryPercentage': 1.0}],
            'yAxes': [{'stacked': True}]
        },
        'legend': {
            'display': True
        },
        'tooltips': {
            'intersect': False,
            'mode': 'x'
        }
    }, title='Tarballs from GitHub')

    rows = binaries.get_tagged_data(binaries_db)
    tag_dict = binaries.get_downloads_by_field(rows, 'rosdistro', 'os')

    chart['data']['labels'] = list(map(str, tag_dict.keys()))

    all_os = collections.Counter()
    for key in tag_dict:
        all_os.update(tag_dict[key])

    for os, _ in all_os.most_common():
        values = []
        for key in tag_dict:
            values.append(tag_dict[key].get(os))
        chart.add(os, values)

    return chart


def get_wiki_chart():
    wiki_db = MetricDB('wiki')
    chart = Chart('line', title='Total wiki.ros.org Pages and Edits')
    chart.add('pages', get_regular_unique_series(wiki_db, 'revisions', 'date', 'page_id'))

    buckets = collections.Counter()
    results = wiki_db.query('SELECT date FROM revisions ORDER BY date')
    for result in results:
        dt = epoch_to_datetime(result['date'])
        m = dt.month - ((dt.month - 1) % 4)
        key = dt.year, m
        buckets[key] += 1
    series = []
    for k, v in sorted(buckets.items()):
        series.append((year_month_to_datetime(*k), v))
    chart.add('edits per quarter', series)
    return chart


def get_commits_chart(commits_db=None):
    if commits_db is None:
        commits_db = MetricDB('commits')
    chart = Chart('line', title='Number of Commits')
    chart.add('All Repos', get_regular_aggregate_series(commits_db, 'commits', 'date', clause='AND valid == 1'))
    return chart
