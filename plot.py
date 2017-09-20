#!/usr/bin/env python3

import csv
import sys
from matplotlib.pyplot import plot, show, legend, title, savefig, clf
from dateutil import parser  # pip install python-dateutil

PLOTS = [
    ('Website Metrics', ['Homepage Views', 'Installation Views', 'Tutorials Views']),
    ('Website by Country', ['US Views', 'China Views', 'Germany Views', 'Japan Views']),
    ('ROS Answers Website Metrics', ['questions hits', 'ask hits']),
    ('Communication Platforms', ['users subscribers', 'wiki.ros.org users', 'answers users', 'discourse users']),
    ('Wiki Stats', ['wiki pages', 'wiki page views']),
    ('ROS Answers Stats', ['Questions', 'Answered Questions']),
]
SINGLE_PLOTS = ['Total Downloads', 'Unique IPs', 'Papers Citing']

D = csv.DictReader(open('aggregated.csv'))
ROWS = {}
for row in D:
    a = []
    for key in D.fieldnames[1:]:
        try:
            a.append(float(row[key]))
        except:
            a.append(None)
    ROWS[row['']] = a

save_to_file = '-s' in sys.argv
c = 0

dates = [parser.parse(d) for d in D.fieldnames[1:]]
for name, keys in PLOTS:
    for key in keys:
        plot(dates, ROWS[key], 'o-', label=key)
    legend(loc=0)
    title(name)
    if save_to_file:
        savefig('%02d - %s.png' % (c, name))
        c += 1
        clf()
    else:
        show()

for key in SINGLE_PLOTS:
    plot(dates, ROWS[key], 'o-', label=key)
    title(key)
    if save_to_file:
        savefig('%02d - %s.png' % (c, key))
        c += 1
        clf()
    else:
        show()
