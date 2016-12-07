import csv
from matplotlib.pyplot import plot, show, legend, title
from dateutil import parser # pip install python-dateutil
import collections

PLOTS = [
  ('Website Metrics', ['Homepage Views', 'Installation Views', 'Tutorials Views']),
  ('Website by Country', ['US Views', 'China Views', 'Germany Views', 'Japan Views']),
  ('ROS Answers Website Metrics', ['questions hits', 'ask hits']),
  ('Communication Platforms', ['users subscribers', 'wiki.ros.org users', 'answers users', 'discourse users']),
  ('Wiki Stats', ['wiki pages', 'wiki page views']),
  ('ROS Answers Stats', ['Questions', 'Answered Questions']),
]
SINGLE_PLOTS = ['Total Downloads', 'Unique IPSs','Papers Citing']

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
    
dates = [parser.parse(a) for a in D.fieldnames[1:]]
for name, keys in PLOTS:
    for key in keys:
        plot(dates, ROWS[key], 'o-', label=key)
    legend()
    title(name)
    show()
for key in SINGLE_PLOTS:
    plot(dates, ROWS[key], 'o-', label=key)
    title(key)
    show()