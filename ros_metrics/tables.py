import collections

from . import repos


class Table(object):
    def __init__(self, headers=None):
        if headers is None:
            self.headers = []
        else:
            self.headers = headers
        self.data = []
        self.column_defs = []

    def add_renderer(self, column, function_name):
        target = self.headers.index(column)
        cdef = {'render': function_name, 'targets': target}
        self.column_defs.append(cdef)

    def add_rank_data(self, columns):
        ranks = collections.defaultdict(collections.Counter)
        for row in self.data:
            for column in columns:
                ranks[column][row[column]] += 1

        cached_ranks = collections.defaultdict(dict)
        for column in columns:
            rank = 1
            for value, ct in sorted(ranks[column].items(), reverse=True):
                cached_ranks[column][value] = rank
                rank += ct

        for row in self.data:
            my_ranks = {}
            for column in columns:
                my_value = row[column]
                my_ranks[column] = cached_ranks[column][my_value]

            product = 1
            for key, value in my_ranks.items():
                product *= value
                new_key = key[:-1] + '_rank'
                row[new_key] = value
            row['rank_product'] = product

        display_columns = []
        hidden_columns = []

        for column in columns:
            display_columns.append(len(self.headers))
            self.headers.append(column)
            hidden_columns.append(len(self.headers))
            new_key = column[:-1] + '_rank'
            self.headers.append(new_key)

        cdef = {'render': 'rankRenderer', 'targets': display_columns, 'orderSequence': ['desc', 'asc']}
        self.column_defs.append(cdef)
        cdef = {'visible': False, 'targets': hidden_columns}
        self.column_defs.append(cdef)


def github_repos_report():
    table = Table(['rank_product', 'org', 'repo'])
    table.data = repos.github_repos_report()

    table.add_renderer('repo', 'linkRenderer')
    table.add_rank_data(['forks', 'stars', 'subs'])
    table.headers += ['open issues', 'closed issues', 'total issues',
                      'open prs', 'merged prs', 'closed prs', 'total prs']

    return table
