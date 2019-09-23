#!/usr/bin/env python3

import argparse
from ros_metrics.metric_db import MetricDB
from tabulate import tabulate

parser = argparse.ArgumentParser()
parser.add_argument('folder')
parser.add_argument('n', nargs='?', default=10, type=int)
args = parser.parse_args()

db = MetricDB(args.folder)

for table in db.db_structure['tables']:
    print(table)
    print(tabulate(db.query('SELECT * FROM {} LIMIT {}'.format(table, args.n)), headers='keys'))
    print()
